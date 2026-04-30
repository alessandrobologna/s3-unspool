use std::env;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::StalledStreamProtectionConfig;
use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};
use s3_unspool::{
    ObjectReport, OperationStatus, PutDiagnostics, S3Object, S3Prefix, SyncDiagnostics,
    SyncOptions, SyncReport, UploadOptions, UploadProgress, UploadProgressHandler, UploadReport,
    sync_zip_to_s3, upload_directory_zip_to_s3,
};
use serde::Serialize;
use terminal_size::{Width, terminal_size};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        let _ = writeln!(io::stderr().lock(), "× {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .with_ansi(false)
        .with_target(false)
        .init();

    let matches = cli().get_matches_from(env::args_os());
    let output = Output::from_matches(&matches);
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .stalled_stream_protection(
                StalledStreamProtectionConfig::enabled()
                    .upload_enabled(false)
                    .download_enabled(true)
                    .build(),
            )
            .build(),
    );

    match matches.subcommand() {
        Some(("extract", matches)) => run_extract(&client, matches, &output).await,
        Some(("upload", matches)) => run_upload(&client, matches, &output).await,
        _ => unreachable!("subcommand is required by clap"),
    }
}

async fn run_extract(
    client: &aws_sdk_s3::Client,
    matches: &ArgMatches,
    output: &Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = matches
        .get_one::<String>("source")
        .expect("required by clap");
    let destination = matches
        .get_one::<String>("destination")
        .expect("required by clap");
    let concurrency = *matches
        .get_one::<usize>("concurrency")
        .expect("defaulted by clap");
    let delete_extra = matches.get_flag("delete-extra");
    let diagnostics = matches.get_flag("diagnostics");
    let ignore_catalog = matches.get_flag("ignore-catalog");
    let report_destination = ReportDestination::from_cli_value(matches.get_one::<String>("report"));

    output.write(&Transcript::running(
        "Extract",
        vec![
            format!("{source} -> {destination}"),
            format!(
                "{} workers{}",
                concurrency,
                if delete_extra { ", delete extra" } else { "" }
            ),
        ],
    ))?;

    let mut options = SyncOptions::new(S3Object::parse(source)?, S3Prefix::parse(destination)?);
    options.concurrency = concurrency;
    options.delete_extra = delete_extra;
    options.collect_diagnostics = diagnostics;
    options.collect_operations = !matches!(report_destination, ReportDestination::None);
    options.ignore_embedded_catalog = ignore_catalog;

    let activity = output.start_activity("Extracting", None);
    let report = sync_zip_to_s3(client, options).await;
    activity.finish().await;
    let report = report?;
    write_report(&report_destination, &report).await?;
    output.write(&extract_transcript(&report, &report_destination))?;

    if report.has_errors() {
        return Err(
            "extract completed with per-object errors; rerun with --report for details".into(),
        );
    }

    Ok(())
}

async fn run_upload(
    client: &aws_sdk_s3::Client,
    matches: &ArgMatches,
    output: &Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_dir = matches
        .get_one::<PathBuf>("source-dir")
        .expect("required by clap");
    let destination = matches
        .get_one::<String>("destination-zip")
        .expect("required by clap");
    let report_destination = ReportDestination::from_cli_value(matches.get_one::<String>("report"));

    output.write(&Transcript::running(
        "Upload",
        vec![format!("{} -> {destination}", source_dir.display())],
    ))?;

    let progress_detail = ActivityDetail::default();
    let mut options = UploadOptions::new(source_dir, S3Object::parse(destination)?);
    let progress_sink = progress_detail.clone();
    options.progress = Some(UploadProgressHandler::new(move |progress| {
        progress_sink.set(upload_progress_state(&progress));
    }));
    let activity = output.start_activity("Uploading", Some(progress_detail));
    let upload_started = Instant::now();
    let report = upload_directory_zip_to_s3(client, options).await;
    activity.finish().await;
    let upload_elapsed = upload_started.elapsed();
    let report = report?;
    write_report(&report_destination, &report).await?;
    output.write(&upload_transcript(
        &report,
        &report_destination,
        upload_elapsed,
    ))?;

    Ok(())
}

async fn write_report<T: Serialize>(
    destination: &ReportDestination,
    report: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    if let ReportDestination::JsonFile(path) = destination {
        let json = serde_json::to_string_pretty(report)?;
        tokio::fs::write(PathBuf::from(path), json).await?;
    }

    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReportDestination {
    None,
    Human,
    JsonFile(String),
}

impl ReportDestination {
    fn from_cli_value(value: Option<&String>) -> Self {
        match value.map(String::as_str) {
            None => Self::None,
            Some("-") => Self::Human,
            Some(path) => Self::JsonFile(path.to_string()),
        }
    }
}

fn cli() -> Command {
    Command::new("s3-unspool")
        .about("Upload local directories as S3 ZIPs and extract S3 ZIPs into S3 prefixes")
        .arg_required_else_help(true)
        .subcommand_required(true)
        .arg(
            Arg::new("quiet")
                .long("quiet")
                .short('q')
                .global(true)
                .help("Suppress human-readable status output")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("color")
                .long("color")
                .global(true)
                .value_name("WHEN")
                .help("Control color output")
                .value_parser(["auto", "always", "never"])
                .default_value("auto"),
        )
        .subcommand(
            Command::new("extract")
                .about("Extract missing or changed files from an S3 ZIP object into an S3 prefix")
                .arg(
                    Arg::new("source")
                        .value_name("SOURCE_ZIP")
                        .help("Source ZIP object, for example s3://bucket/archive.zip")
                        .required(true),
                )
                .arg(
                    Arg::new("destination")
                        .value_name("DESTINATION_PREFIX")
                        .help("Destination S3 prefix, for example s3://bucket/prefix/")
                        .required(true),
                )
                .arg(
                    Arg::new("delete-extra")
                        .long("delete-extra")
                        .help("Delete destination objects under the prefix that are not present in the ZIP")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("concurrency")
                        .long("concurrency")
                        .value_name("N")
                        .help("Maximum number of ZIP entries to process at once")
                        .value_parser(value_parser!(usize))
                        .default_value("64"),
                )
                .arg(
                    Arg::new("report")
                        .long("report")
                        .value_name("PATH_OR_-")
                        .num_args(0..=1)
                        .require_equals(true)
                        .default_missing_value("-")
                        .help("Show a formatted operation report, or write JSON to a path with --report=PATH"),
                )
                .arg(
                    Arg::new("diagnostics")
                        .long("diagnostics")
                        .help("Collect aggregate source range, block cache, and PUT retry diagnostics in the JSON report")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("ignore-catalog")
                        .long("ignore-catalog")
                        .help("Ignore the embedded MD5 catalog and use the fallback extract-and-hash comparison path")
                        .action(ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("upload")
                .about("Zip a local directory and upload the archive to S3")
                .arg(
                    Arg::new("source-dir")
                        .value_name("LOCAL_DIR")
                        .help("Local directory whose contents should be zipped")
                        .value_parser(value_parser!(PathBuf))
                        .required(true),
                )
                .arg(
                    Arg::new("destination-zip")
                        .value_name("DESTINATION_ZIP")
                        .help("Destination ZIP object, for example s3://bucket/archive.zip")
                        .required(true),
                )
                .arg(
                    Arg::new("report")
                        .long("report")
                        .value_name("PATH_OR_-")
                        .num_args(0..=1)
                        .require_equals(true)
                        .default_missing_value("-")
                        .help("Show a formatted upload report, or write JSON to a path with --report=PATH"),
                ),
        )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ColorMode {
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug)]
struct Theme {
    color: bool,
}

impl Theme {
    fn from_mode(mode: ColorMode, stderr_is_terminal: bool) -> Self {
        let color = match mode {
            ColorMode::Auto => stderr_is_terminal && env::var_os("NO_COLOR").is_none(),
            ColorMode::Always => true,
            ColorMode::Never => false,
        };
        Self { color }
    }

    fn dim(self, value: &str) -> String {
        self.wrap("2", value)
    }

    fn muted_hint(self, value: &str) -> String {
        self.wrap("90", value)
    }

    fn brightness(self, level: f64, value: &str) -> String {
        if !self.color {
            return value.to_string();
        }

        let channel = (150.0 + 85.0 * level.clamp(0.0, 1.0)).round() as u8;
        format!("\x1b[38;2;{channel};{channel};{channel}m{value}\x1b[0m")
    }

    fn accent(self, value: &str) -> String {
        self.wrap("36", value)
    }

    fn success(self, value: &str) -> String {
        self.wrap("32", value)
    }

    fn error(self, value: &str) -> String {
        self.wrap("31", value)
    }

    fn wrap(self, code: &str, value: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{value}\x1b[0m")
        } else {
            value.to_string()
        }
    }
}

#[derive(Debug)]
struct Output {
    quiet: bool,
    theme: Theme,
    interactive: bool,
}

impl Output {
    fn from_matches(matches: &ArgMatches) -> Self {
        let stderr_is_terminal = io::stderr().is_terminal();
        let quiet = matches.get_flag("quiet");
        let color = match matches
            .get_one::<String>("color")
            .map(String::as_str)
            .unwrap_or("auto")
        {
            "always" => ColorMode::Always,
            "never" => ColorMode::Never,
            _ => ColorMode::Auto,
        };
        Self {
            quiet,
            theme: Theme::from_mode(color, stderr_is_terminal),
            interactive: stderr_is_terminal,
        }
    }

    fn write(&self, transcript: &Transcript) -> io::Result<()> {
        if self.quiet && transcript.kind != TranscriptKind::Error {
            return Ok(());
        }
        let rendered = transcript.render(self.theme);
        writeln!(io::stderr().lock(), "{rendered}")
    }

    fn start_activity(&self, verb: &'static str, detail: Option<ActivityDetail>) -> Activity {
        if self.quiet || !self.interactive {
            return Activity::disabled();
        }

        Activity::start(verb, self.theme, detail)
    }
}

#[derive(Clone, Debug, Default)]
struct ActivityDetail {
    value: Arc<Mutex<Option<ActivityProgress>>>,
}

impl ActivityDetail {
    fn set(&self, value: ActivityProgress) {
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
struct ActivityProgress {
    processed_files: usize,
    total_files: usize,
    current_file: Option<usize>,
    processed_bytes: u64,
    total_bytes: u64,
}

struct Activity {
    done: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Activity {
    fn disabled() -> Self {
        Self {
            done: Arc::new(AtomicBool::new(true)),
            handle: None,
        }
    }

    fn start(verb: &'static str, theme: Theme, detail: Option<ActivityDetail>) -> Self {
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

    async fn finish(mut self) {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TranscriptKind {
    Running,
    Success,
    Notice,
    Error,
}

#[derive(Debug, Eq, PartialEq)]
struct Transcript {
    kind: TranscriptKind,
    title: String,
    details: Vec<String>,
}

impl Transcript {
    fn running(title: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            kind: TranscriptKind::Running,
            title: title.into(),
            details,
        }
    }

    fn success(title: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            kind: TranscriptKind::Success,
            title: title.into(),
            details,
        }
    }

    fn notice(title: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            kind: TranscriptKind::Notice,
            title: title.into(),
            details,
        }
    }

    fn error(title: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            kind: TranscriptKind::Error,
            title: title.into(),
            details,
        }
    }

    fn render(&self, theme: Theme) -> String {
        let symbol = match self.kind {
            TranscriptKind::Running => theme.accent("•"),
            TranscriptKind::Success => theme.success("✓"),
            TranscriptKind::Notice => theme.accent("!"),
            TranscriptKind::Error => theme.error("×"),
        };
        let mut lines = vec![format!("{symbol} {}", self.title)];

        for (index, detail) in self.details.iter().enumerate() {
            let prefix = if index == 0 { "  └ " } else { "    " };
            lines.push(format!("{}{}", theme.muted_hint(prefix), detail));
        }

        lines.join("\n")
    }
}

fn upload_transcript(
    report: &UploadReport,
    report_destination: &ReportDestination,
    upload_elapsed: Duration,
) -> Transcript {
    let mut details = vec![
        format!(
            "{}, {} uncompressed, {} ZIP",
            plural(report.files, "file", "files"),
            format_bytes(report.uncompressed_bytes),
            format_bytes(report.zip_bytes)
        ),
        report.destination.uri(),
    ];
    match report_destination {
        ReportDestination::None => {}
        ReportDestination::JsonFile(path) => details.push(format!("Report: {path}")),
        ReportDestination::Human => details.extend(upload_report_details(report, upload_elapsed)),
    }

    Transcript::success("Upload complete", details)
}

fn extract_transcript(report: &SyncReport, report_destination: &ReportDestination) -> Transcript {
    let summary = &report.summary;
    let mut details = if summary.uploaded_new == 0
        && summary.uploaded_changed == 0
        && summary.deleted_extra == 0
        && summary.conditional_conflicts == 0
        && summary.errors == 0
    {
        vec![format!(
            "{} unchanged",
            plural(summary.skipped_unchanged, "file", "files")
        )]
    } else {
        vec![format!(
            "{}: {} new, {} changed, {} unchanged",
            plural(summary.zip_files, "file", "files"),
            summary.uploaded_new,
            summary.uploaded_changed,
            summary.skipped_unchanged
        )]
    };

    details.push(format!(
        "Destination: {} listed",
        plural(summary.destination_objects, "object", "objects")
    ));
    if summary.deleted_extra > 0 {
        details.push(format!(
            "Deleted: {} extra",
            plural(summary.deleted_extra, "object", "objects")
        ));
    }
    if summary.conditional_conflicts > 0 {
        details.push(format!(
            "Conflicts: {}",
            plural(
                summary.conditional_conflicts,
                "conditional write",
                "conditional writes"
            )
        ));
    }
    if summary.errors > 0 {
        details.push(format!(
            "Errors: {}",
            plural(summary.errors, "object", "objects")
        ));
    }
    if let Some(diagnostics) = &report.diagnostics {
        details.push(diagnostics_line(diagnostics));
    }
    match report_destination {
        ReportDestination::None => {}
        ReportDestination::JsonFile(path) => details.push(format!("Report: {path}")),
        ReportDestination::Human => details.extend(extract_report_details(report)),
    }

    if summary.errors > 0 {
        Transcript::error("Extract completed with errors", details)
    } else if summary.conditional_conflicts > 0 {
        Transcript::notice("Extract completed with conflicts", details)
    } else if summary.uploaded_new == 0
        && summary.uploaded_changed == 0
        && summary.deleted_extra == 0
    {
        Transcript::success("Up to date", details)
    } else {
        Transcript::success("Extract complete", details)
    }
}

fn upload_report_details(report: &UploadReport, upload_elapsed: Duration) -> Vec<String> {
    vec![
        "Report:".to_string(),
        format!("  Source: {}", report.source_dir),
        format!("  Destination: {}", report.destination.uri()),
        format!("  Files: {}", plural(report.files, "file", "files")),
        format!(
            "  Uncompressed: {}",
            format_bytes(report.uncompressed_bytes)
        ),
        format!("  ZIP: {}", format_bytes(report.zip_bytes)),
        format!("  Wall time: {}", format_elapsed(upload_elapsed)),
        format!(
            "  Upload speed: {}",
            format_upload_speed(report.zip_bytes, upload_elapsed)
        ),
    ]
}

fn extract_report_details(report: &SyncReport) -> Vec<String> {
    let summary = &report.summary;
    let mut details = vec![
        "Report:".to_string(),
        format!("  Source: {}", report.source.uri()),
        format!("  Destination: {}", report.destination.uri()),
        format!(
            "  ZIP files: {}",
            plural(summary.zip_files, "file", "files")
        ),
        format!(
            "  Destination listed: {}",
            plural(summary.destination_objects, "object", "objects")
        ),
        format!(
            "  Operations: {} new, {} changed, {} unchanged, {} deleted",
            summary.uploaded_new,
            summary.uploaded_changed,
            summary.skipped_unchanged,
            summary.deleted_extra
        ),
    ];

    if summary.conditional_conflicts > 0 || summary.errors > 0 {
        details.push(format!(
            "  Issues: {} conflicts, {} errors",
            summary.conditional_conflicts, summary.errors
        ));
    }
    if let Some(diagnostics) = &report.diagnostics {
        details.push(format!("  {}", diagnostics_line(diagnostics)));
    }

    details.extend(operation_report_details(&report.operations));
    details
}

fn operation_report_details(operations: &[ObjectReport]) -> Vec<String> {
    let noteworthy = operations
        .iter()
        .filter(|operation| operation.status != OperationStatus::SkippedUnchanged)
        .collect::<Vec<_>>();

    if noteworthy.is_empty() {
        return Vec::new();
    }

    let shown = noteworthy.len().min(10);
    let mut details = vec!["  Objects:".to_string()];
    details.extend(
        noteworthy
            .iter()
            .take(shown)
            .map(|operation| format!("    {}", operation_report_line(operation))),
    );
    if noteworthy.len() > shown {
        details.push(format!(
            "    ... {} more",
            plural(noteworthy.len() - shown, "object", "objects")
        ));
    }
    details
}

fn operation_report_line(operation: &ObjectReport) -> String {
    let status = match operation.status {
        OperationStatus::UploadedNew => "uploaded new",
        OperationStatus::UploadedChanged => "uploaded changed",
        OperationStatus::SkippedUnchanged => "unchanged",
        OperationStatus::ConditionalConflict => "conflict",
        OperationStatus::DeletedExtra => "deleted extra",
        OperationStatus::Error => "error",
    };
    let size = operation
        .size
        .map(format_bytes)
        .map(|size| format!(" ({size})"))
        .unwrap_or_default();
    let message = operation
        .message
        .as_ref()
        .map(|message| format!(": {}", truncate_text(message, 72)))
        .unwrap_or_default();
    format!("{status}: {}{size}{message}", operation.key)
}

fn diagnostics_line(diagnostics: &SyncDiagnostics) -> String {
    let mut line = format!(
        "Source: {} GET attempts, {} blocks fetched, {} waits, {:.2}x amplification",
        diagnostics.source.source_get_attempts,
        diagnostics.source.fetched_blocks,
        diagnostics.source.block_waits,
        diagnostics.source.source_amplification
    );
    if diagnostics.put.failed_attempts > 0 {
        line.push_str(&format!(
            ", {} PUT failures, {} retries ({})",
            diagnostics.put.failed_attempts,
            diagnostics.put.retry_attempts,
            format_put_failure_codes(&diagnostics.put)
        ));
    }
    line
}

fn format_put_failure_codes(diagnostics: &PutDiagnostics) -> String {
    diagnostics
        .failures_by_error_code
        .iter()
        .map(|(code, count)| format!("{code}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn upload_progress_state(progress: &UploadProgress) -> ActivityProgress {
    match progress {
        UploadProgress::Planned {
            total_files,
            total_bytes,
        } => ActivityProgress {
            processed_files: 0,
            total_files: *total_files,
            current_file: None,
            processed_bytes: 0,
            total_bytes: *total_bytes,
        },
        UploadProgress::FileStarted {
            current_file,
            total_files,
            processed_files,
            processed_bytes,
            total_bytes,
            ..
        } => ActivityProgress {
            processed_files: *processed_files,
            total_files: *total_files,
            current_file: Some(*current_file),
            processed_bytes: *processed_bytes,
            total_bytes: *total_bytes,
        },
        UploadProgress::FileProgress {
            current_file,
            total_files,
            processed_files,
            processed_bytes,
            total_bytes,
            ..
        } => ActivityProgress {
            processed_files: *processed_files,
            total_files: *total_files,
            current_file: Some(*current_file),
            processed_bytes: *processed_bytes,
            total_bytes: *total_bytes,
        },
        UploadProgress::FileFinished {
            processed_files,
            total_files,
            processed_bytes,
            total_bytes,
            ..
        } => ActivityProgress {
            processed_files: *processed_files,
            total_files: *total_files,
            current_file: None,
            processed_bytes: *processed_bytes,
            total_bytes: *total_bytes,
        },
        UploadProgress::Finished {
            total_files,
            total_bytes,
        } => ActivityProgress {
            processed_files: *total_files,
            total_files: *total_files,
            current_file: None,
            processed_bytes: *total_bytes,
            total_bytes: *total_bytes,
        },
    }
}

fn render_progress(
    theme: Theme,
    progress: &ActivityProgress,
    available_width: usize,
    animation_frame: usize,
) -> String {
    let percent = progress_percent(progress);
    let bytes = byte_progress(progress.processed_bytes, progress.total_bytes);
    let files = progress_file_label(progress);
    let metadata = format!("{percent} {bytes} {files}");

    let max_bar_inner_width = available_width.saturating_sub(metadata.len() + 3);
    if max_bar_inner_width < 6 {
        let compact = format!("{percent} {files}");
        if available_width < compact.len() {
            return percent;
        }
        return compact;
    }

    let bar_inner_width = max_bar_inner_width.min(18);
    format!(
        "{} {metadata}",
        progress_bar(theme, progress, bar_inner_width, animation_frame)
    )
}

fn progress_bar(
    theme: Theme,
    progress: &ActivityProgress,
    inner_width: usize,
    animation_frame: usize,
) -> String {
    format!(
        "[{}]",
        progress_bar_cells(
            theme,
            progress_filled_eighths(progress, inner_width),
            inner_width,
            animation_frame,
        )
    )
}

fn progress_bar_cells(
    theme: Theme,
    filled_eighths: usize,
    inner_width: usize,
    animation_frame: usize,
) -> String {
    let mut cells = String::new();
    for index in 0..inner_width {
        let cell_eighths = filled_eighths.saturating_sub(index * 8).min(8);
        let block = progress_block(cell_eighths).to_string();
        if cell_eighths == 0 {
            cells.push_str(&theme.muted_hint(&block));
        } else {
            cells.push_str(&theme.brightness(shimmer_brightness(index, animation_frame), &block));
        }
    }
    cells
}

fn progress_block(eighths: usize) -> char {
    match eighths.min(8) {
        0 => ' ',
        1 => '▏',
        2 => '▎',
        3 => '▍',
        4 => '▌',
        5 => '▋',
        6 => '▊',
        7 => '▉',
        _ => '█',
    }
}

fn shimmer_brightness(index: usize, animation_frame: usize) -> f64 {
    let phase = ((index as f64 - animation_frame as f64 * 0.5) / 6.0) * std::f64::consts::TAU;
    0.5 + 0.5 * phase.cos()
}

fn progress_filled_eighths(progress: &ActivityProgress, width: usize) -> usize {
    if width == 0 {
        return 0;
    }
    if progress.total_bytes == 0 {
        return if progress.processed_files >= progress.total_files {
            width * 8
        } else {
            0
        };
    }

    let processed = progress.processed_bytes.min(progress.total_bytes) as u128;
    let total = progress.total_bytes as u128;
    ((processed * (width * 8) as u128) / total) as usize
}

fn progress_percent(progress: &ActivityProgress) -> String {
    let percent = if progress.total_bytes == 0 {
        if progress.processed_files >= progress.total_files {
            100
        } else {
            0
        }
    } else {
        let processed = progress.processed_bytes.min(progress.total_bytes) as u128;
        let total = progress.total_bytes as u128;
        ((processed * 100) / total) as u64
    };
    format!("{percent}%")
}

fn progress_file_label(progress: &ActivityProgress) -> String {
    if progress.total_files == 0 {
        return "0 files".to_string();
    }
    if let Some(current_file) = progress.current_file {
        return format!("file {current_file}/{}", progress.total_files);
    }
    file_progress(progress.processed_files, progress.total_files)
}

fn file_progress(processed_files: usize, total_files: usize) -> String {
    if total_files == 0 {
        "0 files".to_string()
    } else {
        format!("{processed_files}/{total_files} files")
    }
}

fn byte_progress(processed_bytes: u64, total_bytes: u64) -> String {
    if total_bytes == 0 {
        return format_bytes(processed_bytes);
    }

    format!(
        "{}/{}",
        format_bytes(processed_bytes),
        format_bytes(total_bytes)
    )
}

fn plural(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }

    let keep = max_chars.saturating_sub(3);
    format!("{}...", value.chars().take(keep).collect::<String>())
}

fn format_elapsed(elapsed: Duration) -> String {
    let total_seconds = elapsed.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    format!("{minutes:02}:{seconds:02}")
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn format_upload_speed(bytes: u64, elapsed: Duration) -> String {
    let seconds = elapsed.as_secs_f64();
    let mib_per_second = if seconds > 0.0 {
        bytes as f64 / 1024.0 / 1024.0 / seconds
    } else {
        0.0
    };
    format!("{mib_per_second:.2} MiB/s")
}

#[cfg(test)]
mod tests {
    use s3_unspool::{
        PutRetryDiagnostics, RetryJitter, SourceDiagnostics, SyncDiagnostics, SyncSummary,
    };

    use super::*;

    #[test]
    fn parses_extract_subcommand() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "--quiet",
                "--color",
                "never",
                "extract",
                "--delete-extra",
                "--ignore-catalog",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        assert!(matches.get_flag("quiet"));
        assert_eq!(
            matches.get_one::<String>("color").map(String::as_str),
            Some("never")
        );

        let Some(("extract", extract)) = matches.subcommand() else {
            panic!("expected extract subcommand");
        };
        assert!(extract.get_flag("delete-extra"));
        assert!(extract.get_flag("ignore-catalog"));
        assert_eq!(
            extract.get_one::<String>("source").map(String::as_str),
            Some("s3://source-bucket/archive.zip")
        );
    }

    #[test]
    fn parses_upload_subcommand() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "upload",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("upload", upload)) = matches.subcommand() else {
            panic!("expected upload subcommand");
        };
        assert_eq!(
            upload.get_one::<PathBuf>("source-dir"),
            Some(&PathBuf::from("/tmp/site"))
        );
        assert_eq!(
            upload
                .get_one::<String>("destination-zip")
                .map(String::as_str),
            Some("s3://destination-bucket/site.zip")
        );
    }

    #[test]
    fn parses_bare_report_as_stdout_for_extract() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "extract",
                "--report",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        let Some(("extract", extract)) = matches.subcommand() else {
            panic!("expected extract subcommand");
        };
        assert_eq!(
            extract.get_one::<String>("report").map(String::as_str),
            Some("-")
        );
        assert_eq!(
            extract.get_one::<String>("source").map(String::as_str),
            Some("s3://source-bucket/archive.zip")
        );
    }

    #[test]
    fn parses_report_path_for_extract() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "extract",
                "--report=report.json",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        let Some(("extract", extract)) = matches.subcommand() else {
            panic!("expected extract subcommand");
        };
        assert_eq!(
            extract.get_one::<String>("report").map(String::as_str),
            Some("report.json")
        );
    }

    #[test]
    fn parses_report_path_after_extract_positionals() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "extract",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
                "--report=report.json",
            ])
            .unwrap();

        let Some(("extract", extract)) = matches.subcommand() else {
            panic!("expected extract subcommand");
        };
        assert_eq!(
            extract.get_one::<String>("report").map(String::as_str),
            Some("report.json")
        );
    }

    #[test]
    fn parses_bare_report_before_inline_value_options() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "extract",
                "--report",
                "--concurrency=32",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        let Some(("extract", extract)) = matches.subcommand() else {
            panic!("expected extract subcommand");
        };
        assert_eq!(
            extract.get_one::<String>("report").map(String::as_str),
            Some("-")
        );
        assert_eq!(extract.get_one::<usize>("concurrency"), Some(&32));
        assert_eq!(
            extract.get_one::<String>("source").map(String::as_str),
            Some("s3://source-bucket/archive.zip")
        );
    }

    #[test]
    fn parses_bare_report_as_stdout_for_upload() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "upload",
                "--report",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("upload", upload)) = matches.subcommand() else {
            panic!("expected upload subcommand");
        };
        assert_eq!(
            upload.get_one::<String>("report").map(String::as_str),
            Some("-")
        );
        assert_eq!(
            upload.get_one::<PathBuf>("source-dir"),
            Some(&PathBuf::from("/tmp/site"))
        );
    }

    #[test]
    fn parses_report_dash_as_stdout_for_upload() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "upload",
                "--report=-",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("upload", upload)) = matches.subcommand() else {
            panic!("expected upload subcommand");
        };
        assert_eq!(
            upload.get_one::<String>("report").map(String::as_str),
            Some("-")
        );
    }

    #[test]
    fn parses_report_path_for_upload() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "upload",
                "--report=report.json",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("upload", upload)) = matches.subcommand() else {
            panic!("expected upload subcommand");
        };
        assert_eq!(
            upload.get_one::<String>("report").map(String::as_str),
            Some("report.json")
        );
    }

    #[test]
    fn renders_upload_transcript() {
        let report = UploadReport {
            source_dir: "./site".to_string(),
            destination: S3Object::parse("s3://bucket/site.zip").unwrap(),
            files: 2,
            uncompressed_bytes: 1536,
            zip_bytes: 768,
        };

        let transcript = upload_transcript(
            &report,
            &ReportDestination::JsonFile("report.json".to_string()),
            Duration::from_secs(2),
        );

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Upload complete\n  └ 2 files, 1.5 KiB uncompressed, 768 B ZIP\n    s3://bucket/site.zip\n    Report: report.json"
        );
    }

    #[test]
    fn renders_upload_transcript_with_human_report() {
        let report = UploadReport {
            source_dir: "./site".to_string(),
            destination: S3Object::parse("s3://bucket/site.zip").unwrap(),
            files: 2,
            uncompressed_bytes: 4 * 1024 * 1024,
            zip_bytes: 3 * 1024 * 1024,
        };

        let transcript =
            upload_transcript(&report, &ReportDestination::Human, Duration::from_secs(2));

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Upload complete\n  └ 2 files, 4.0 MiB uncompressed, 3.0 MiB ZIP\n    s3://bucket/site.zip\n    Report:\n      Source: ./site\n      Destination: s3://bucket/site.zip\n      Files: 2 files\n      Uncompressed: 4.0 MiB\n      ZIP: 3.0 MiB\n      Wall time: 00:02\n      Upload speed: 1.50 MiB/s"
        );
    }

    #[test]
    fn renders_up_to_date_extract_transcript() {
        let report = SyncReport {
            source: S3Object::parse("s3://bucket/site.zip").unwrap(),
            destination: S3Prefix::parse("s3://bucket/www/").unwrap(),
            summary: SyncSummary {
                zip_files: 10,
                destination_objects: 10,
                skipped_unchanged: 10,
                ..SyncSummary::default()
            },
            diagnostics: None,
            operations: Vec::new(),
        };

        let transcript = extract_transcript(&report, &ReportDestination::None);

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Up to date\n  └ 10 files unchanged\n    Destination: 10 objects listed"
        );
    }

    #[test]
    fn renders_changed_extract_transcript_with_diagnostics() {
        let report = SyncReport {
            source: S3Object::parse("s3://bucket/site.zip").unwrap(),
            destination: S3Prefix::parse("s3://bucket/www/").unwrap(),
            summary: SyncSummary {
                zip_files: 10,
                destination_objects: 12,
                uploaded_new: 1,
                uploaded_changed: 2,
                skipped_unchanged: 7,
                deleted_extra: 1,
                ..SyncSummary::default()
            },
            diagnostics: Some(SyncDiagnostics {
                concurrency: 64,
                put_concurrency: 8,
                put_retry: PutRetryDiagnostics {
                    max_attempts: 6,
                    base_delay_ms: 250,
                    max_delay_ms: 5_000,
                    slowdown_base_delay_ms: 1_000,
                    slowdown_max_delay_ms: 30_000,
                    jitter: RetryJitter::Full,
                },
                source_block_size: 8192,
                source_block_merge_gap: 1024,
                source_get_concurrency: 4,
                source_window_capacity: 4096,
                source: SourceDiagnostics {
                    source_zip_bytes: 100,
                    planned_entries: 3,
                    planned_blocks: 2,
                    fetched_blocks: 2,
                    source_get_attempts: 3,
                    source_get_retries: 0,
                    source_get_request_errors: 0,
                    source_get_body_errors: 0,
                    source_get_short_body_errors: 0,
                    source_get_errors: 0,
                    planned_source_bytes: 100,
                    fetched_source_bytes: 100,
                    unique_source_bytes: 80,
                    source_amplification: 1.25,
                    block_hits: 8,
                    block_waits: 1,
                    block_releases: 2,
                    block_misses: 1,
                    block_refetches: 0,
                    active_gets_high_water: 2,
                },
                put: PutDiagnostics::default(),
            }),
            operations: Vec::new(),
        };

        let transcript = extract_transcript(
            &report,
            &ReportDestination::JsonFile("report.json".to_string()),
        );

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Extract complete\n  └ 10 files: 1 new, 2 changed, 7 unchanged\n    Destination: 12 objects listed\n    Deleted: 1 object extra\n    Source: 3 GET attempts, 2 blocks fetched, 1 waits, 1.25x amplification\n    Report: report.json"
        );
    }

    #[test]
    fn renders_extract_transcript_with_human_report() {
        let report = SyncReport {
            source: S3Object::parse("s3://bucket/site.zip").unwrap(),
            destination: S3Prefix::parse("s3://bucket/www/").unwrap(),
            summary: SyncSummary {
                zip_files: 3,
                destination_objects: 2,
                uploaded_new: 1,
                uploaded_changed: 1,
                skipped_unchanged: 1,
                ..SyncSummary::default()
            },
            diagnostics: None,
            operations: vec![
                ObjectReport {
                    status: OperationStatus::UploadedNew,
                    key: "www/a.txt".to_string(),
                    zip_path: Some("a.txt".to_string()),
                    size: Some(10),
                    md5: None,
                    destination_etag: None,
                    message: None,
                },
                ObjectReport {
                    status: OperationStatus::UploadedChanged,
                    key: "www/b.txt".to_string(),
                    zip_path: Some("b.txt".to_string()),
                    size: Some(20),
                    md5: None,
                    destination_etag: Some("old".to_string()),
                    message: None,
                },
                ObjectReport {
                    status: OperationStatus::SkippedUnchanged,
                    key: "www/c.txt".to_string(),
                    zip_path: Some("c.txt".to_string()),
                    size: Some(30),
                    md5: None,
                    destination_etag: Some("same".to_string()),
                    message: None,
                },
            ],
        };

        let transcript = extract_transcript(&report, &ReportDestination::Human);

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Extract complete\n  └ 3 files: 1 new, 1 changed, 1 unchanged\n    Destination: 2 objects listed\n    Report:\n      Source: s3://bucket/site.zip\n      Destination: s3://bucket/www/\n      ZIP files: 3 files\n      Destination listed: 2 objects\n      Operations: 1 new, 1 changed, 1 unchanged, 0 deleted\n      Objects:\n        uploaded new: www/a.txt (10 B)\n        uploaded changed: www/b.txt (20 B)"
        );
    }

    #[test]
    fn renders_conflict_extract_transcript() {
        let report = SyncReport {
            source: S3Object::parse("s3://bucket/site.zip").unwrap(),
            destination: S3Prefix::parse("s3://bucket/www/").unwrap(),
            summary: SyncSummary {
                zip_files: 3,
                destination_objects: 3,
                uploaded_changed: 1,
                skipped_unchanged: 1,
                conditional_conflicts: 1,
                ..SyncSummary::default()
            },
            diagnostics: None,
            operations: Vec::new(),
        };

        let transcript = extract_transcript(&report, &ReportDestination::None);

        assert_eq!(
            transcript.render(Theme { color: false }),
            "! Extract completed with conflicts\n  └ 3 files: 0 new, 1 changed, 1 unchanged\n    Destination: 3 objects listed\n    Conflicts: 1 conditional write"
        );
    }

    #[test]
    fn formats_bytes_for_summary_rows() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(768), "768 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_bytes(10 * 1024), "10 KiB");
        assert_eq!(format_bytes(2 * 1024 * 1024), "2.0 MiB");
    }

    #[test]
    fn formats_upload_speed_with_two_decimal_places() {
        assert_eq!(
            format_upload_speed(3 * 1024 * 1024, Duration::from_secs(2)),
            "1.50 MiB/s"
        );
        assert_eq!(
            format_upload_speed(1024 * 1024, Duration::from_secs(0)),
            "0.00 MiB/s"
        );
    }

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

    #[test]
    fn formats_elapsed_as_stable_minutes_and_seconds() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "00:00");
        assert_eq!(format_elapsed(Duration::from_secs(9)), "00:09");
        assert_eq!(format_elapsed(Duration::from_secs(75)), "01:15");
    }

    #[test]
    fn renders_upload_progress_bar() {
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&UploadProgress::Planned {
                    total_files: 10,
                    total_bytes: 100,
                }),
                80,
                0,
            ),
            "[                  ] 0% 0 B/100 B 0/10 files"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&UploadProgress::FileFinished {
                    processed_files: 3,
                    total_files: 10,
                    processed_bytes: 30,
                    total_bytes: 100,
                    path: "a.txt".to_string(),
                }),
                80,
                0,
            ),
            "[█████▍            ] 30% 30 B/100 B 3/10 files"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&UploadProgress::FileStarted {
                    current_file: 4,
                    total_files: 10,
                    processed_files: 3,
                    processed_bytes: 30,
                    total_bytes: 100,
                    path: "b.txt".to_string(),
                }),
                80,
                0,
            ),
            "[█████▍            ] 30% 30 B/100 B file 4/10"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&UploadProgress::FileProgress {
                    current_file: 4,
                    total_files: 10,
                    processed_files: 3,
                    processed_bytes: 40,
                    total_bytes: 100,
                    path: "b.txt".to_string(),
                }),
                80,
                0,
            ),
            "[███████▏          ] 40% 40 B/100 B file 4/10"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&UploadProgress::Finished {
                    total_files: 10,
                    total_bytes: 100,
                }),
                80,
                0,
            ),
            "[██████████████████] 100% 100 B/100 B 10/10 files"
        );
    }

    #[test]
    fn maps_upload_progress_events_to_activity_state() {
        assert_eq!(
            upload_progress_state(&UploadProgress::Planned {
                total_files: 10,
                total_bytes: 100,
            }),
            ActivityProgress {
                processed_files: 0,
                total_files: 10,
                current_file: None,
                processed_bytes: 0,
                total_bytes: 100,
            }
        );
        assert_eq!(
            upload_progress_state(&UploadProgress::FileFinished {
                processed_files: 3,
                total_files: 10,
                processed_bytes: 30,
                total_bytes: 100,
                path: "a.txt".to_string(),
            }),
            ActivityProgress {
                processed_files: 3,
                total_files: 10,
                current_file: None,
                processed_bytes: 30,
                total_bytes: 100,
            }
        );
        assert_eq!(
            upload_progress_state(&UploadProgress::FileStarted {
                current_file: 4,
                total_files: 10,
                processed_files: 3,
                processed_bytes: 30,
                total_bytes: 100,
                path: "b.txt".to_string(),
            }),
            ActivityProgress {
                processed_files: 3,
                total_files: 10,
                current_file: Some(4),
                processed_bytes: 30,
                total_bytes: 100,
            }
        );
        assert_eq!(
            upload_progress_state(&UploadProgress::Finished {
                total_files: 10,
                total_bytes: 100,
            }),
            ActivityProgress {
                processed_files: 10,
                total_files: 10,
                current_file: None,
                processed_bytes: 100,
                total_bytes: 100,
            }
        );
    }

    #[test]
    fn calculates_progress_bar_fill_from_bytes() {
        let progress = ActivityProgress {
            processed_files: 3,
            total_files: 10,
            current_file: Some(4),
            processed_bytes: 40,
            total_bytes: 100,
        };

        assert_eq!(progress_filled_eighths(&progress, 18), 57);
        assert_eq!(
            progress_bar_cells(Theme { color: false }, 57, 18, 0),
            "███████▏          "
        );
        assert_eq!(progress_block(1), '▏');
        assert_eq!(progress_block(4), '▌');
        assert_eq!(progress_block(7), '▉');
        assert_eq!(progress_block(8), '█');
    }

    #[test]
    fn calculates_cosine_shimmer_brightness() {
        assert!((shimmer_brightness(0, 0) - 1.0).abs() < f64::EPSILON);
        assert!((shimmer_brightness(0, 6) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn renders_compact_progress_when_width_is_tight() {
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&UploadProgress::FileProgress {
                    current_file: 4,
                    total_files: 10,
                    processed_files: 3,
                    processed_bytes: 40,
                    total_bytes: 100,
                    path: "b.txt".to_string(),
                }),
                18,
                0,
            ),
            "40% file 4/10"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&UploadProgress::FileProgress {
                    current_file: 4,
                    total_files: 10,
                    processed_files: 3,
                    processed_bytes: 40,
                    total_bytes: 100,
                    path: "b.txt".to_string(),
                }),
                4,
                0,
            ),
            "40%"
        );
    }
}
