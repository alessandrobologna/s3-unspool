use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use aws_sdk_s3::Client;
use bytes::Bytes;
use futures_util::FutureExt;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncSeek, ReadBuf, SeekFrom};
use tokio::sync::futures::OwnedNotified;
use tokio::sync::{Notify, Semaphore};

use crate::constants::GET_OBJECT_MAX_ATTEMPTS;
use crate::error::aws_error_message;
use crate::report::SourceDiagnostics;
use crate::zip_manifest::ManifestEntry;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct SourceRange {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

impl SourceRange {
    pub(crate) fn len(self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }

    fn end_exclusive(self) -> u64 {
        self.end.saturating_add(1)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SourcePlan {
    pub(crate) planned_entries: usize,
    pub(crate) blocks: Vec<SourceRange>,
}

#[derive(Debug)]
pub(crate) struct SourceDiagnosticsCollector {
    source_zip_bytes: u64,
    planned_entries: AtomicU64,
    planned_blocks: AtomicU64,
    fetched_blocks: AtomicU64,
    source_get_attempts: AtomicU64,
    source_get_retries: AtomicU64,
    source_get_request_errors: AtomicU64,
    source_get_body_errors: AtomicU64,
    source_get_short_body_errors: AtomicU64,
    source_get_errors: AtomicU64,
    planned_source_bytes: AtomicU64,
    fetched_source_bytes: AtomicU64,
    block_hits: AtomicU64,
    block_waits: AtomicU64,
    block_releases: AtomicU64,
    block_misses: AtomicU64,
    block_refetches: AtomicU64,
    active_gets: AtomicUsize,
    active_gets_high_water: AtomicUsize,
    fetched_coverage: Mutex<RangeCoverage>,
}

impl SourceDiagnosticsCollector {
    pub(crate) fn new(source_zip_bytes: u64) -> Self {
        Self {
            source_zip_bytes,
            planned_entries: AtomicU64::new(0),
            planned_blocks: AtomicU64::new(0),
            fetched_blocks: AtomicU64::new(0),
            source_get_attempts: AtomicU64::new(0),
            source_get_retries: AtomicU64::new(0),
            source_get_request_errors: AtomicU64::new(0),
            source_get_body_errors: AtomicU64::new(0),
            source_get_short_body_errors: AtomicU64::new(0),
            source_get_errors: AtomicU64::new(0),
            planned_source_bytes: AtomicU64::new(0),
            fetched_source_bytes: AtomicU64::new(0),
            block_hits: AtomicU64::new(0),
            block_waits: AtomicU64::new(0),
            block_releases: AtomicU64::new(0),
            block_misses: AtomicU64::new(0),
            block_refetches: AtomicU64::new(0),
            active_gets: AtomicUsize::new(0),
            active_gets_high_water: AtomicUsize::new(0),
            fetched_coverage: Mutex::new(RangeCoverage::default()),
        }
    }

    pub(crate) fn record_plan(&self, plan: &SourcePlan) {
        self.planned_entries
            .fetch_add(plan.planned_entries as u64, Ordering::Relaxed);
        self.planned_blocks
            .fetch_add(plan.blocks.len() as u64, Ordering::Relaxed);
        self.planned_source_bytes.fetch_add(
            plan.blocks.iter().map(|range| range.len()).sum::<u64>(),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_get_attempt(&self, _start: u64, _end: u64, attempt: usize) {
        self.source_get_attempts.fetch_add(1, Ordering::Relaxed);
        if attempt > 1 {
            self.source_get_retries.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_get_success(&self, start: u64, end: u64) {
        self.fetched_source_bytes.fetch_add(
            end.saturating_sub(start).saturating_add(1),
            Ordering::Relaxed,
        );
        self.fetched_coverage
            .lock()
            .expect("source diagnostics mutex is not poisoned")
            .insert(start, end);
    }

    fn record_get_request_error(&self) {
        self.source_get_request_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_get_body_error(&self) {
        self.source_get_body_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_get_short_body_error(&self) {
        self.source_get_short_body_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_get_error(&self) {
        self.source_get_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_block_fetched(&self) {
        self.fetched_blocks.fetch_add(1, Ordering::Relaxed);
    }

    fn record_block_hit(&self) {
        self.block_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_block_wait(&self) {
        self.block_waits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_block_release(&self) {
        self.block_releases.fetch_add(1, Ordering::Relaxed);
    }

    fn record_block_refetch(&self) {
        self.block_refetches.fetch_add(1, Ordering::Relaxed);
    }

    fn enter_get(&self) -> ActiveGetGuard<'_> {
        let active = self.active_gets.fetch_add(1, Ordering::Relaxed) + 1;
        update_high_water(&self.active_gets_high_water, active);
        ActiveGetGuard { diagnostics: self }
    }

    pub(crate) fn active_gets(&self) -> u64 {
        self.active_gets.load(Ordering::Relaxed) as u64
    }

    pub(crate) fn snapshot(&self) -> SourceDiagnostics {
        let unique_source_bytes = self
            .fetched_coverage
            .lock()
            .expect("source diagnostics mutex is not poisoned")
            .unique_bytes();
        let fetched_source_bytes = self.fetched_source_bytes.load(Ordering::Relaxed);
        let source_amplification = if unique_source_bytes == 0 {
            0.0
        } else {
            fetched_source_bytes as f64 / unique_source_bytes as f64
        };

        SourceDiagnostics {
            source_zip_bytes: self.source_zip_bytes,
            planned_entries: self.planned_entries.load(Ordering::Relaxed),
            planned_blocks: self.planned_blocks.load(Ordering::Relaxed),
            fetched_blocks: self.fetched_blocks.load(Ordering::Relaxed),
            source_get_attempts: self.source_get_attempts.load(Ordering::Relaxed),
            source_get_retries: self.source_get_retries.load(Ordering::Relaxed),
            source_get_request_errors: self.source_get_request_errors.load(Ordering::Relaxed),
            source_get_body_errors: self.source_get_body_errors.load(Ordering::Relaxed),
            source_get_short_body_errors: self.source_get_short_body_errors.load(Ordering::Relaxed),
            source_get_errors: self.source_get_errors.load(Ordering::Relaxed),
            planned_source_bytes: self.planned_source_bytes.load(Ordering::Relaxed),
            fetched_source_bytes,
            unique_source_bytes,
            source_amplification,
            block_hits: self.block_hits.load(Ordering::Relaxed),
            block_waits: self.block_waits.load(Ordering::Relaxed),
            block_releases: self.block_releases.load(Ordering::Relaxed),
            block_misses: self.block_misses.load(Ordering::Relaxed),
            block_refetches: self.block_refetches.load(Ordering::Relaxed),
            active_gets_high_water: self.active_gets_high_water.load(Ordering::Relaxed) as u64,
        }
    }
}

struct ActiveGetGuard<'a> {
    diagnostics: &'a SourceDiagnosticsCollector,
}

impl Drop for ActiveGetGuard<'_> {
    fn drop(&mut self) {
        self.diagnostics.active_gets.fetch_sub(1, Ordering::Relaxed);
    }
}

fn update_high_water(value: &AtomicUsize, candidate: usize) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

#[derive(Debug, Default)]
struct RangeCoverage {
    intervals: BTreeMap<u64, u64>,
    unique_bytes: u64,
}

impl RangeCoverage {
    fn insert(&mut self, start: u64, end: u64) {
        if end < start {
            return;
        }

        let mut merged_start = start;
        let mut merged_end = end;

        if let Some((&current_start, &current_end)) = self.intervals.range(..=start).next_back()
            && current_end.saturating_add(1) >= start
        {
            merged_start = current_start;
            merged_end = merged_end.max(current_end);
            self.remove_interval(current_start, current_end);
        }

        while let Some((current_start, current_end)) =
            self.next_overlapping_interval(merged_start, merged_end)
        {
            merged_start = merged_start.min(current_start);
            merged_end = merged_end.max(current_end);
            self.remove_interval(current_start, current_end);
        }

        self.unique_bytes = self
            .unique_bytes
            .saturating_add(merged_end.saturating_sub(merged_start).saturating_add(1));
        self.intervals.insert(merged_start, merged_end);
    }

    fn unique_bytes(&self) -> u64 {
        self.unique_bytes
    }

    fn next_overlapping_interval(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        let upper = end.saturating_add(1);
        self.intervals
            .range(start..=upper)
            .next()
            .map(|(&current_start, &current_end)| (current_start, current_end))
    }

    fn remove_interval(&mut self, start: u64, end: u64) {
        self.intervals.remove(&start);
        self.unique_bytes = self
            .unique_bytes
            .saturating_sub(end.saturating_sub(start).saturating_add(1));
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SourceClient {
    pub(crate) client: Client,
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) len: u64,
    pub(crate) etag: Option<String>,
    pub(crate) diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
}

impl SourceClient {
    pub(crate) async fn get_range(&self, start: u64, end: u64) -> io::Result<Bytes> {
        if end < start {
            return Err(invalid_source_range(start, end));
        }

        let mut last_error = None;
        for attempt in 1..=GET_OBJECT_MAX_ATTEMPTS {
            if let Some(diagnostics) = &self.diagnostics {
                diagnostics.record_get_attempt(start, end, attempt);
            }
            match self.fetch_range_once(start, end).await {
                Ok(bytes) => {
                    if let Some(diagnostics) = &self.diagnostics {
                        diagnostics.record_get_success(start, end);
                    }
                    return Ok(bytes);
                }
                Err(err) if attempt < GET_OBJECT_MAX_ATTEMPTS => {
                    last_error = Some(err);
                    tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
                }
                Err(err) => {
                    if let Some(diagnostics) = &self.diagnostics {
                        diagnostics.record_get_error();
                    }
                    return Err(err);
                }
            }
        }

        if let Some(diagnostics) = &self.diagnostics {
            diagnostics.record_get_error();
        }
        Err(last_error.unwrap_or_else(|| io::Error::other("S3 ranged GetObject failed")))
    }

    async fn fetch_range_once(&self, start: u64, end: u64) -> io::Result<Bytes> {
        let _active_get = self
            .diagnostics
            .as_ref()
            .map(|diagnostics| diagnostics.enter_get());
        let mut request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(format!("bytes={start}-{end}"));

        if let Some(etag) = &self.etag {
            request = request.if_match(etag);
        }

        let output = request.send().await.map_err(|err| {
            if let Some(diagnostics) = &self.diagnostics {
                diagnostics.record_get_request_error();
            }
            io::Error::other(format!(
                "S3 ranged GetObject failed: {}",
                aws_error_message(&err)
            ))
        })?;

        output
            .body
            .collect()
            .await
            .map(|bytes| bytes.into_bytes())
            .map_err(|err| {
                if let Some(diagnostics) = &self.diagnostics {
                    diagnostics.record_get_body_error();
                }
                io::Error::other(format!("S3 body read failed: {err}"))
            })
            .and_then(|bytes| {
                let expected_len = usize::try_from(end - start + 1).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "S3 range is too large")
                })?;
                if bytes.len() == expected_len {
                    Ok(bytes)
                } else {
                    if let Some(diagnostics) = &self.diagnostics {
                        diagnostics.record_get_short_body_error();
                    }
                    Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "S3 range bytes={start}-{end} returned {} bytes, expected {expected_len}",
                            bytes.len()
                        ),
                    ))
                }
            })
    }
}

pub(crate) struct BlockStore {
    blocks: Vec<BlockSlot>,
    state: Mutex<BlockStoreState>,
    capacity: u64,
    capacity_notify: Arc<Notify>,
    source: Option<Arc<SourceClient>>,
    fetch_semaphore: Arc<Semaphore>,
    diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
}

impl BlockStore {
    #[cfg(test)]
    pub(crate) fn new(
        plan: SourcePlan,
        window_capacity: usize,
        diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
    ) -> Arc<Self> {
        if let Some(diagnostics) = &diagnostics {
            diagnostics.record_plan(&plan);
        }
        let blocks = plan
            .blocks
            .into_iter()
            .map(|range| BlockSlot {
                range,
                notify: Arc::new(Notify::new()),
            })
            .collect::<Vec<_>>();
        let claim_counts = vec![0; blocks.len()];
        Self::build(blocks, claim_counts, window_capacity, diagnostics, None, 1)
    }

    pub(crate) fn with_source(
        plan: SourcePlan,
        entries: &[ManifestEntry],
        window_capacity: usize,
        diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
        source: Arc<SourceClient>,
        source_get_concurrency: usize,
    ) -> Arc<Self> {
        if let Some(diagnostics) = &diagnostics {
            diagnostics.record_plan(&plan);
        }
        let blocks = plan
            .blocks
            .into_iter()
            .map(|range| BlockSlot {
                range,
                notify: Arc::new(Notify::new()),
            })
            .collect::<Vec<_>>();
        let claim_counts = initial_claim_counts(&blocks, entries);
        Self::build(
            blocks,
            claim_counts,
            window_capacity,
            diagnostics,
            Some(source),
            source_get_concurrency,
        )
    }

    fn build(
        blocks: Vec<BlockSlot>,
        claim_counts: Vec<usize>,
        window_capacity: usize,
        diagnostics: Option<Arc<SourceDiagnosticsCollector>>,
        source: Option<Arc<SourceClient>>,
        source_get_concurrency: usize,
    ) -> Arc<Self> {
        let refs = if claim_counts.len() == blocks.len() {
            claim_counts
        } else {
            vec![0; blocks.len()]
        };
        Arc::new(Self {
            blocks,
            state: Mutex::new(BlockStoreState {
                slots: refs
                    .into_iter()
                    .map(|remaining_claims| BlockState {
                        remaining_claims,
                        live_claims: 0,
                        fetch_count: 0,
                        status: BlockStatus::Pending,
                    })
                    .collect(),
                resident_bytes: 0,
            }),
            capacity: window_capacity as u64,
            capacity_notify: Arc::new(Notify::new()),
            source,
            fetch_semaphore: Arc::new(Semaphore::new(source_get_concurrency.max(1))),
            diagnostics,
        })
    }

    #[cfg(test)]
    pub(crate) fn retain_entry(&self, entry: &ManifestEntry) {
        let indices = self.block_indices_for_span(entry.source_span_start, entry.source_span_end);
        self.add_remaining_claims(&indices);
    }

    #[cfg(test)]
    fn add_remaining_claims(&self, indices: &[usize]) {
        let mut state = self
            .state
            .lock()
            .expect("block store mutex is not poisoned");
        for &index in indices {
            state.slots[index].remaining_claims =
                state.slots[index].remaining_claims.saturating_add(1);
        }
    }

    fn add_replay_claims(&self, indices: &[usize]) {
        let mut state = self
            .state
            .lock()
            .expect("block store mutex is not poisoned");
        for &index in indices {
            let Some(slot) = state.slots.get_mut(index) else {
                continue;
            };
            slot.remaining_claims = slot.remaining_claims.saturating_add(1);
            if matches!(slot.status, BlockStatus::Released | BlockStatus::Failed(_)) {
                slot.status = BlockStatus::Pending;
            }
        }
    }

    pub(crate) fn start_entry_replay(
        self: &Arc<Self>,
        entry: &ManifestEntry,
    ) -> tokio::task::JoinHandle<()> {
        let indices = self.block_indices_for_span(entry.source_span_start, entry.source_span_end);
        self.add_replay_claims(&indices);
        let store = Arc::clone(self);
        tokio::spawn(async move {
            let mut tasks = Vec::with_capacity(indices.len());
            for index in indices {
                let Some(range) = store.reserve_fetch(index).await else {
                    continue;
                };
                let store = Arc::clone(&store);
                tasks.push(tokio::spawn(async move {
                    let _ = store.fetch_reserved_block(index, range).await;
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn add_replay_claim_for_test(&self, entry: &ManifestEntry) {
        let indices = self.block_indices_for_span(entry.source_span_start, entry.source_span_end);
        self.add_replay_claims(&indices);
    }

    fn activate_block_indices(&self, indices: &[usize]) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .expect("block store mutex is not poisoned");
        for &index in indices {
            let Some(slot) = state.slots.get(index) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "source claim references an unknown block",
                ));
            };
            if slot.remaining_claims == 0 {
                return Err(io::Error::other(
                    "source block has no remaining planned claims",
                ));
            }
            if matches!(slot.status, BlockStatus::Released) {
                return Err(io::Error::other(
                    "source block was already released before the reader was admitted",
                ));
            }
        }
        for &index in indices {
            state.slots[index].live_claims = state.slots[index].live_claims.saturating_add(1);
        }
        Ok(())
    }

    pub(crate) fn block_indices_for_span(&self, start: u64, end_exclusive: u64) -> Vec<usize> {
        if start >= end_exclusive {
            return Vec::new();
        }
        block_indices_for_span(&self.blocks, start, end_exclusive)
    }

    pub(crate) fn block_count(&self) -> usize {
        self.blocks.len()
    }

    #[cfg(test)]
    pub(crate) fn block_range(&self, index: usize) -> Option<SourceRange> {
        self.blocks.get(index).map(|block| block.range)
    }

    #[cfg(test)]
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.state
            .lock()
            .expect("block store mutex is not poisoned")
            .resident_bytes
    }

    #[cfg(test)]
    pub(crate) fn is_block_ready(&self, index: usize) -> bool {
        self.state
            .lock()
            .expect("block store mutex is not poisoned")
            .slots
            .get(index)
            .is_some_and(|slot| matches!(slot.status, BlockStatus::Ready(_)))
    }

    pub(crate) async fn reserve_fetch(&self, index: usize) -> Option<SourceRange> {
        self.reserve_fetch_inner(index).await
    }

    pub(crate) fn finish_fetch(&self, index: usize, result: io::Result<Bytes>) {
        let Some(block) = self.blocks.get(index) else {
            return;
        };
        let notify = Arc::clone(&block.notify);
        let mut release_capacity = false;
        {
            let mut state = self
                .state
                .lock()
                .expect("block store mutex is not poisoned");
            match result {
                Ok(bytes) => {
                    if let Some(diagnostics) = &self.diagnostics {
                        diagnostics.record_block_fetched();
                    }
                    state.slots[index].fetch_count =
                        state.slots[index].fetch_count.saturating_add(1);
                    if state.slots[index].remaining_claims == 0
                        && state.slots[index].live_claims == 0
                    {
                        state.resident_bytes =
                            state.resident_bytes.saturating_sub(block.range.len());
                        state.slots[index].status = BlockStatus::Released;
                        if let Some(diagnostics) = &self.diagnostics {
                            diagnostics.record_block_release();
                        }
                        release_capacity = true;
                    } else {
                        state.slots[index].status = BlockStatus::Ready(bytes);
                    }
                }
                Err(err) => {
                    state.resident_bytes = state.resident_bytes.saturating_sub(block.range.len());
                    state.slots[index].status = BlockStatus::Failed(err.to_string());
                    release_capacity = true;
                }
            }
        }
        notify.notify_waiters();
        if release_capacity {
            self.capacity_notify.notify_waiters();
        }
    }

    pub(crate) async fn slice_from(
        &self,
        position: u64,
        end_exclusive: u64,
    ) -> io::Result<BlockSlice> {
        let index = self
            .block_index_at(position)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "source block missing"))?;
        let block = &self.blocks[index];
        let slice_end_exclusive = block.range.end_exclusive().min(end_exclusive);

        loop {
            let action = {
                let state = self
                    .state
                    .lock()
                    .expect("block store mutex is not poisoned");
                match &state.slots[index].status {
                    BlockStatus::Ready(bytes) => {
                        if let Some(diagnostics) = &self.diagnostics {
                            diagnostics.record_block_hit();
                        }
                        let bytes = bytes.clone();
                        let offset =
                            usize::try_from(position - block.range.start).map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "source offset too large",
                                )
                            })?;
                        let len =
                            usize::try_from(slice_end_exclusive - position).map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "source range too large",
                                )
                            })?;
                        let end = offset.checked_add(len).ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidInput, "source range overflowed")
                        })?;
                        return Ok(BlockSlice {
                            start: position,
                            bytes: bytes.slice(offset..end),
                        });
                    }
                    BlockStatus::Failed(message) => {
                        return Err(io::Error::other(message.clone()));
                    }
                    BlockStatus::Released => {
                        return Err(io::Error::other(
                            "source block was released before all claimed bytes were consumed",
                        ));
                    }
                    BlockStatus::Fetching | BlockStatus::Pending => {
                        if let Some(diagnostics) = &self.diagnostics {
                            diagnostics.record_block_wait();
                        }
                        BlockReadAction::WaitBlock(enabled_notification(&block.notify))
                    }
                }
            };
            match action {
                BlockReadAction::WaitBlock(wait) => wait.await,
            }
        }
    }

    pub(crate) fn release_block_reader(&self, index: usize) {
        if self.blocks.get(index).is_none() {
            return;
        }
        let mut notify_capacity = false;
        {
            let mut state = self
                .state
                .lock()
                .expect("block store mutex is not poisoned");
            let slot = &mut state.slots[index];
            if slot.live_claims == 0 {
                return;
            }
            slot.live_claims -= 1;
            slot.remaining_claims = slot.remaining_claims.saturating_sub(1);
            if slot.live_claims == 0
                && slot.remaining_claims == 0
                && matches!(slot.status, BlockStatus::Ready(_))
            {
                slot.status = BlockStatus::Released;
                state.resident_bytes = state
                    .resident_bytes
                    .saturating_sub(self.blocks[index].range.len());
                if let Some(diagnostics) = &self.diagnostics {
                    diagnostics.record_block_release();
                }
                notify_capacity = true;
            }
        }
        if notify_capacity {
            self.capacity_notify.notify_waiters();
        }
    }

    fn block_index_at(&self, position: u64) -> Option<usize> {
        self.blocks
            .iter()
            .position(|block| block.range.start <= position && position <= block.range.end)
    }

    fn block_end(&self, index: usize) -> Option<u64> {
        self.blocks.get(index).map(|block| block.range.end)
    }

    async fn reserve_fetch_inner(&self, index: usize) -> Option<SourceRange> {
        self.blocks.get(index)?;
        loop {
            let action = {
                let mut state = self
                    .state
                    .lock()
                    .expect("block store mutex is not poisoned");
                match self.reserve_fetch_locked(&mut state, index) {
                    ReserveFetchResult::Reserved(range) => return Some(range),
                    ReserveFetchResult::WaitForCapacity => {
                        Some(enabled_notification(&self.capacity_notify))
                    }
                    ReserveFetchResult::AlreadyReady
                    | ReserveFetchResult::AlreadyFetching
                    | ReserveFetchResult::NoClaims
                    | ReserveFetchResult::Failed => return None,
                }
            };
            if let Some(wait) = action {
                wait.await;
            }
        }
    }

    fn reserve_fetch_locked(
        &self,
        state: &mut BlockStoreState,
        index: usize,
    ) -> ReserveFetchResult {
        let Some(block) = self.blocks.get(index) else {
            return ReserveFetchResult::Failed;
        };
        if state.slots[index].remaining_claims == 0 {
            return ReserveFetchResult::NoClaims;
        }
        match state.slots[index].status {
            BlockStatus::Pending => {}
            BlockStatus::Fetching => return ReserveFetchResult::AlreadyFetching,
            BlockStatus::Ready(_) => return ReserveFetchResult::AlreadyReady,
            BlockStatus::Released => return ReserveFetchResult::NoClaims,
            BlockStatus::Failed(_) => return ReserveFetchResult::Failed,
        }

        let len = block.range.len();
        let target_capacity = self.capacity.max(len);
        if state.resident_bytes.saturating_add(len) > target_capacity {
            return ReserveFetchResult::WaitForCapacity;
        }

        if state.slots[index].fetch_count > 0
            && let Some(diagnostics) = &self.diagnostics
        {
            diagnostics.record_block_refetch();
        }
        state.resident_bytes = state.resident_bytes.saturating_add(len);
        state.slots[index].status = BlockStatus::Fetching;
        ReserveFetchResult::Reserved(block.range)
    }

    async fn fetch_reserved_block(&self, index: usize, range: SourceRange) -> io::Result<()> {
        let Some(source) = self.source.as_ref() else {
            let err = io::Error::other("source block fetcher is not configured");
            self.finish_fetch(index, Err(io::Error::new(err.kind(), err.to_string())));
            return Err(err);
        };
        let _permit = self
            .fetch_semaphore
            .acquire()
            .await
            .map_err(|_| io::Error::other("source fetch semaphore is closed"))?;
        let result = source.get_range(range.start, range.end).await;
        let output = result
            .as_ref()
            .map(|_| ())
            .map_err(|err| io::Error::new(err.kind(), err.to_string()));
        self.finish_fetch(index, result);
        output
    }
}

struct BlockSlot {
    range: SourceRange,
    notify: Arc<Notify>,
}

fn initial_claim_counts(blocks: &[BlockSlot], entries: &[ManifestEntry]) -> Vec<usize> {
    let mut counts = vec![0_usize; blocks.len()];
    for entry in entries {
        for index in block_indices_for_span(blocks, entry.source_span_start, entry.source_span_end)
        {
            counts[index] = counts[index].saturating_add(1);
        }
    }
    counts
}

fn block_indices_for_span(blocks: &[BlockSlot], start: u64, end_exclusive: u64) -> Vec<usize> {
    blocks
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            (block.range.start < end_exclusive && start < block.range.end_exclusive())
                .then_some(index)
        })
        .collect()
}

struct BlockStoreState {
    slots: Vec<BlockState>,
    resident_bytes: u64,
}

struct BlockState {
    remaining_claims: usize,
    live_claims: usize,
    fetch_count: u64,
    status: BlockStatus,
}

enum BlockStatus {
    Pending,
    Fetching,
    Ready(Bytes),
    Released,
    Failed(String),
}

enum ReserveFetchResult {
    Reserved(SourceRange),
    WaitForCapacity,
    AlreadyReady,
    AlreadyFetching,
    NoClaims,
    Failed,
}

enum BlockReadAction {
    WaitBlock(EnabledNotification),
}

type EnabledNotification = Pin<Box<OwnedNotified>>;

fn enabled_notification(notify: &Arc<Notify>) -> EnabledNotification {
    let mut wait = Box::pin(Arc::clone(notify).notified_owned());
    wait.as_mut().enable();
    wait
}

pub(crate) struct BlockSlice {
    pub(crate) start: u64,
    pub(crate) bytes: Bytes,
}

pub(crate) fn start_source_scheduler(store: Arc<BlockStore>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tasks = Vec::with_capacity(store.block_count());

        for index in 0..store.block_count() {
            let Some(range) = store.reserve_fetch(index).await else {
                continue;
            };
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                let _ = store.fetch_reserved_block(index, range).await;
            }));
        }

        for task in tasks {
            let _ = task.await;
        }
    })
}

pub(crate) struct BlockRangeReader {
    store: Arc<BlockStore>,
    position: u64,
    end_exclusive: u64,
    buffer_start: u64,
    buffer: Bytes,
    in_flight: Option<Pin<Box<dyn Future<Output = io::Result<BlockSlice>> + Send>>>,
    remaining_blocks: VecDeque<usize>,
}

impl BlockRangeReader {
    pub(crate) fn new(store: Arc<BlockStore>, start: u64, end_exclusive: u64) -> io::Result<Self> {
        let remaining_blocks = store
            .block_indices_for_span(start, end_exclusive)
            .into_iter()
            .collect::<VecDeque<_>>();
        let indices = remaining_blocks.iter().copied().collect::<Vec<_>>();
        store.activate_block_indices(&indices)?;
        Ok(Self {
            store,
            position: start,
            end_exclusive,
            buffer_start: start,
            buffer: Bytes::new(),
            in_flight: None,
            remaining_blocks,
        })
    }

    fn available(&self) -> Option<&[u8]> {
        let buffer_end = self.buffer_start.saturating_add(self.buffer.len() as u64);
        if self.position >= self.buffer_start && self.position < buffer_end {
            let offset = (self.position - self.buffer_start) as usize;
            Some(&self.buffer[offset..])
        } else {
            None
        }
    }

    fn release_finished_blocks(&mut self) {
        while let Some(index) = self.remaining_blocks.front().copied() {
            let Some(end) = self.store.block_end(index) else {
                self.remaining_blocks.pop_front();
                continue;
            };
            if end < self.position {
                self.remaining_blocks.pop_front();
                self.store.release_block_reader(index);
            } else {
                break;
            }
        }
    }

    fn start_fetch(&mut self) {
        let position = self.position;
        let end_exclusive = self.end_exclusive;
        let store = Arc::clone(&self.store);
        self.in_flight = Some(Box::pin(async move {
            store.slice_from(position, end_exclusive).await
        }));
    }

    fn poll_fetch(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.position >= self.end_exclusive {
            return Poll::Ready(Ok(()));
        }

        if self.in_flight.is_none() {
            self.start_fetch();
        }

        let fetched = match self
            .in_flight
            .as_mut()
            .expect("in-flight source block fetch exists")
            .poll_unpin(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result?,
        };

        self.buffer_start = fetched.start;
        self.buffer = fetched.bytes;
        self.in_flight = None;

        if self.buffer.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source block returned no data before EOF",
            )));
        }

        Poll::Ready(Ok(()))
    }
}

impl Drop for BlockRangeReader {
    fn drop(&mut self) {
        while let Some(index) = self.remaining_blocks.pop_front() {
            self.store.release_block_reader(index);
        }
    }
}

impl AsyncRead for BlockRangeReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position >= self.end_exclusive || buf.remaining() == 0 {
            self.release_finished_blocks();
            return Poll::Ready(Ok(()));
        }

        if self.available().is_none() {
            self.release_finished_blocks();
            std::task::ready!(self.poll_fetch(cx))?;
        }

        let available = self.available().unwrap_or_default();
        let len = available.len().min(buf.remaining());
        buf.put_slice(&available[..len]);
        self.position += len as u64;
        self.release_finished_blocks();
        Poll::Ready(Ok(()))
    }
}

pub(crate) struct S3RangeReader {
    source: Arc<SourceClient>,
    position: u64,
    chunk_size: usize,
    buffer_start: u64,
    buffer: Bytes,
    in_flight: Option<Pin<Box<dyn Future<Output = io::Result<Bytes>> + Send>>>,
    in_flight_start: u64,
}

impl S3RangeReader {
    pub(crate) fn new(source: Arc<SourceClient>, chunk_size: usize) -> Self {
        Self {
            source,
            position: 0,
            chunk_size: chunk_size.max(1),
            buffer_start: 0,
            buffer: Bytes::new(),
            in_flight: None,
            in_flight_start: 0,
        }
    }

    fn available(&self) -> Option<&[u8]> {
        let buffer_end = self.buffer_start.saturating_add(self.buffer.len() as u64);
        if self.position >= self.buffer_start && self.position < buffer_end {
            let offset = (self.position - self.buffer_start) as usize;
            Some(&self.buffer[offset..])
        } else {
            None
        }
    }

    fn start_fetch(&mut self) {
        let chunk_size = self.chunk_size.max(1) as u64;
        let start = align_down(self.position, chunk_size);
        let end = self
            .source
            .len
            .saturating_sub(1)
            .min(start.saturating_add(chunk_size - 1));
        let source = Arc::clone(&self.source);
        self.in_flight_start = start;
        self.in_flight = Some(Box::pin(async move { source.get_range(start, end).await }));
    }

    fn poll_fetch(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.position >= self.source.len {
            return Poll::Ready(Ok(()));
        }

        if self.in_flight.is_none() {
            self.start_fetch();
        }

        let fetched = match self
            .in_flight
            .as_mut()
            .expect("in-flight source fetch exists")
            .poll_unpin(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result?,
        };

        self.buffer_start = self.in_flight_start;
        self.buffer = fetched;
        self.in_flight = None;

        if self.buffer.is_empty() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "S3 range request returned no data before EOF",
            )));
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for S3RangeReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.position >= self.source.len || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        if self.available().is_none() {
            std::task::ready!(self.poll_fetch(cx))?;
        }

        let available = self.available().unwrap_or_default();
        let len = available.len().min(buf.remaining());
        buf.put_slice(&available[..len]);
        self.position += len as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncBufRead for S3RangeReader {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();

        if this.position >= this.source.len {
            return Poll::Ready(Ok(&[]));
        }

        if this.available().is_none() {
            std::task::ready!(this.poll_fetch(cx))?;
        }

        let buffer_end = this.buffer_start.saturating_add(this.buffer.len() as u64);
        if this.position >= this.buffer_start && this.position < buffer_end {
            let offset = (this.position - this.buffer_start) as usize;
            Poll::Ready(Ok(&this.buffer[offset..]))
        } else {
            Poll::Ready(Ok(&[]))
        }
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        let consumed = amt.min(self.available().unwrap_or_default().len());
        self.position = self.position.saturating_add(consumed as u64);
    }
}

impl AsyncSeek for S3RangeReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let len = self.source.len as i128;
        let current = self.position as i128;
        let next = match position {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::End(offset) => len + offset as i128,
            SeekFrom::Current(offset) => current + offset as i128,
        };

        if next < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before start of S3 object",
            ));
        }

        self.position = next as u64;
        self.in_flight = None;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

pub(crate) fn plan_source_blocks(
    entries: &[ManifestEntry],
    source_len: u64,
    block_size: usize,
    merge_gap: usize,
) -> SourcePlan {
    if source_len == 0 {
        return SourcePlan::default();
    }

    let block_size = block_size.max(1) as u64;
    let merge_gap = merge_gap as u64;
    let mut spans = entries
        .iter()
        .filter_map(|entry| {
            let start = entry.source_span_start.min(source_len);
            let end = entry.source_span_end.min(source_len);
            (start < end).then_some((start, end))
        })
        .collect::<Vec<_>>();
    spans.sort_unstable();

    let mut coalesced = Vec::<(u64, u64)>::new();
    for (start, end) in spans.iter().copied() {
        let Some((current_start, current_end)) = coalesced.last_mut() else {
            coalesced.push((start, end));
            continue;
        };
        let gap = start.saturating_sub(*current_end);
        let proposed_end = (*current_end).max(end);
        if gap <= merge_gap && proposed_end.saturating_sub(*current_start) <= block_size {
            *current_end = proposed_end;
        } else {
            coalesced.push((start, end));
        }
    }

    let mut blocks = Vec::new();
    for (start, end) in coalesced {
        let mut block_start = start;
        while block_start < end {
            let block_end_exclusive = block_start.saturating_add(block_size).min(end);
            blocks.push(SourceRange {
                start: block_start,
                end: block_end_exclusive - 1,
            });
            block_start = block_end_exclusive;
        }
    }

    SourcePlan {
        planned_entries: spans.len(),
        blocks,
    }
}

pub(crate) fn align_down(value: u64, block_size: u64) -> u64 {
    value - (value % block_size)
}

fn invalid_source_range(start: u64, end: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("invalid S3 range: start {start} is greater than end {end}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enabled_block_wait_survives_notify_before_await() {
        let notify = Arc::new(Notify::new());
        let wait = enabled_notification(&notify);

        notify.notify_waiters();

        tokio::time::timeout(Duration::from_millis(100), wait)
            .await
            .expect("enabled block waiter should observe an immediate notification");
    }

    #[tokio::test]
    async fn enabled_capacity_wait_survives_notify_before_await() {
        let notify = Arc::new(Notify::new());
        let wait = enabled_notification(&notify);

        notify.notify_waiters();

        tokio::time::timeout(Duration::from_millis(100), wait)
            .await
            .expect("enabled capacity waiter should observe an immediate notification");
    }
}
