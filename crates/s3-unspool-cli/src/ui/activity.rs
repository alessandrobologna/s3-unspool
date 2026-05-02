use std::io::{self, Write};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use terminal_size::{Width, terminal_size};

use super::progress::render_progress;
use super::{Theme, format_elapsed};

#[derive(Clone, Debug, Default)]
pub(crate) struct ActivityDetail {
    value: Arc<Mutex<Option<ActivityProgress>>>,
}

impl ActivityDetail {
    pub(crate) fn set(&self, value: ActivityProgress) {
        *self
            .value
            .lock()
            .expect("activity detail mutex is not poisoned") = Some(value);
    }

    fn get(&self) -> Option<ActivityProgress> {
        self.value
            .lock()
            .expect("activity detail mutex is not poisoned")
            .clone()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ActivityProgress {
    pub(crate) processed_files: usize,
    pub(crate) total_files: usize,
    pub(crate) current_file: Option<usize>,
    pub(crate) processed_bytes: u64,
    pub(crate) total_bytes: u64,
}

pub(crate) struct Activity {
    done: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Activity {
    pub(crate) fn disabled() -> Self {
        Self {
            done: Arc::new(AtomicBool::new(true)),
            handle: None,
        }
    }

    pub(crate) fn start(verb: &'static str, theme: Theme, detail: Option<ActivityDetail>) -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let task_done = Arc::clone(&done);
        let handle = tokio::spawn(async move {
            const FRAMES: [&str; 2] = ["•", "◦"];
            let started = Instant::now();
            let mut frame = 0;

            while !task_done.load(Ordering::Relaxed) {
                let detail = detail.as_ref().and_then(ActivityDetail::get);
                let line = activity_line(
                    theme,
                    FRAMES[frame % FRAMES.len()],
                    verb,
                    started.elapsed(),
                    detail.as_ref(),
                    terminal_width().unwrap_or(100),
                    frame,
                );
                {
                    let mut stderr = io::stderr().lock();
                    let _ = write!(stderr, "\r{line}");
                    let _ = stderr.flush();
                }
                frame = frame.wrapping_add(1);
                tokio::time::sleep(Duration::from_millis(120)).await;
            }

            clear_activity_line();
        });

        Self {
            done,
            handle: Some(handle),
        }
    }

    pub(crate) async fn finish(mut self) {
        self.done.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for Activity {
    fn drop(&mut self) {
        self.done.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            handle.abort();
            clear_activity_line();
        }
    }
}

fn activity_line(
    theme: Theme,
    frame: &str,
    verb: &str,
    elapsed: Duration,
    progress: Option<&ActivityProgress>,
    terminal_width: usize,
    animation_frame: usize,
) -> String {
    let mut line = format!(
        "{} {} {}",
        theme.accent(frame),
        verb,
        theme.dim(&format_elapsed(elapsed))
    );
    if let Some(progress) = progress {
        line.push(' ');
        let used_width = activity_line_prefix_width(frame, verb);
        let available_width = terminal_width.saturating_sub(used_width + 1);
        line.push_str(&render_progress(
            theme,
            progress,
            available_width,
            animation_frame,
        ));
    }
    line
}

fn activity_line_prefix_width(frame: &str, verb: &str) -> usize {
    frame.chars().count() + 1 + verb.len() + 1 + "00:00".len()
}

fn terminal_width() -> Option<usize> {
    terminal_size().map(|(Width(width), _)| usize::from(width))
}

fn clear_activity_line() {
    let mut stderr = io::stderr().lock();
    let _ = write!(stderr, "\r\x1b[2K");
    let _ = stderr.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_activity_line_with_frozen_elapsed() {
        let progress = ActivityProgress {
            processed_files: 3,
            total_files: 10,
            current_file: Some(4),
            processed_bytes: 30,
            total_bytes: 100,
        };

        assert_eq!(
            activity_line(
                Theme { color: false },
                "•",
                "Uploading",
                Duration::from_secs(65),
                Some(&progress),
                80,
                0,
            ),
            "• Uploading 01:05 [█████▍            ] 30% 30 B/100 B file 4/10"
        );
    }

    #[test]
    fn renders_activity_line_without_wrapping_when_narrow() {
        let progress = ActivityProgress {
            processed_files: 3,
            total_files: 10,
            current_file: Some(4),
            processed_bytes: 30,
            total_bytes: 100,
        };

        assert_eq!(
            activity_line(
                Theme { color: false },
                "•",
                "Uploading",
                Duration::from_secs(65),
                Some(&progress),
                36,
                0,
            ),
            "• Uploading 01:05 30% file 4/10"
        );
    }
}
