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
    DryRunObjectReport, DryRunOperationStatus, LocalUnzipOptions, LocalUnzipReport,
    LocalZipOptions, LocalZipReport, LocalZipSyncOptions, LocalZipToS3Report, ObjectReport,
    OperationStatus, PutDiagnostics, S3Object, S3Prefix, S3PrefixLocalZipOptions,
    S3PrefixUploadOptions, S3PrefixUploadReport, S3ZipLocalUnzipOptions, SyncDiagnostics,
    SyncOptions, SyncReport, SyncSummary, UnzipDryRunReport, UnzipDryRunSummary, UploadOptions,
    UploadProgress, UploadProgressHandler, UploadReport, ZipCompression, ZipDryRunReport,
    dry_run_sync_zip_to_s3, dry_run_unzip_file_to_local, dry_run_unzip_file_to_s3,
    dry_run_unzip_s3_zip_to_local, dry_run_upload_directory_zip_to_s3,
    dry_run_zip_directory_to_file, dry_run_zip_s3_prefix_to_file, dry_run_zip_s3_prefix_to_s3,
    sync_zip_to_s3, unzip_file_to_local, unzip_file_to_s3, unzip_s3_zip_to_local,
    upload_directory_zip_to_s3, zip_directory_to_file, zip_s3_prefix_to_file, zip_s3_prefix_to_s3,
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

    match matches.subcommand() {
        Some(("zip", matches)) => run_zip(matches, &output).await,
        Some(("unzip", matches)) => run_unzip(matches, &output).await,
        _ => unreachable!("subcommand is required by clap"),
    }
}

async fn s3_client() -> aws_sdk_s3::Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .stalled_stream_protection(
                StalledStreamProtectionConfig::enabled()
                    .upload_enabled(false)
                    .download_enabled(true)
                    .build(),
            )
            .build(),
    )
}

async fn run_unzip(
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
    let dry_run = matches.get_flag("dry-run");
    let report_destination = ReportDestination::from_cli_value(matches.get_one::<String>("report"));
    let source = parse_zip_source(source)?;
    let destination = parse_tree_destination(destination)?;
    validate_delete_extra_destination(delete_extra, &destination)?;
    validate_diagnostics_source(diagnostics, &source)?;

    output.write(&Transcript::running(
        if dry_run { "Unzip dry run" } else { "Unzip" },
        vec![
            format!("{} -> {}", source.display(), destination.display()),
            format!(
                "{} workers{}{}",
                concurrency,
                if delete_extra { ", delete extra" } else { "" },
                if dry_run { ", no changes" } else { "" }
            ),
        ],
    ))?;

    let activity = output.start_activity(
        if dry_run {
            "Planning unzip"
        } else {
            "Unzipping"
        },
        None,
    );
    let report = match (source, destination) {
        (ZipSource::S3(source), TreeDestination::S3(destination)) => {
            let client = s3_client().await;
            let mut options = SyncOptions::new(source, destination);
            options.concurrency = concurrency;
            options.delete_extra = delete_extra;
            options.collect_diagnostics = diagnostics;
            options.collect_operations = !matches!(report_destination, ReportDestination::None);
            options.ignore_embedded_catalog = ignore_catalog;
            if dry_run {
                UnzipCommandReport::DryRun(dry_run_sync_zip_to_s3(&client, options).await?)
            } else {
                UnzipCommandReport::S3(sync_zip_to_s3(&client, options).await?)
            }
        }
        (ZipSource::Local(source_zip), TreeDestination::S3(destination)) => {
            let client = s3_client().await;
            let mut options = LocalZipSyncOptions::new(source_zip, destination);
            options.concurrency = concurrency;
            options.delete_extra = delete_extra;
            options.collect_operations = !matches!(report_destination, ReportDestination::None);
            options.ignore_embedded_catalog = ignore_catalog;
            if dry_run {
                UnzipCommandReport::DryRun(dry_run_unzip_file_to_s3(&client, options).await?)
            } else {
                UnzipCommandReport::LocalZipToS3(unzip_file_to_s3(&client, options).await?)
            }
        }
        (ZipSource::S3(source), TreeDestination::Local(destination_dir)) => {
            let client = s3_client().await;
            let mut options = S3ZipLocalUnzipOptions::new(source, destination_dir);
            options.concurrency = concurrency;
            options.collect_diagnostics = diagnostics;
            options.collect_operations = !matches!(report_destination, ReportDestination::None);
            options.ignore_embedded_catalog = ignore_catalog;
            if dry_run {
                UnzipCommandReport::DryRun(dry_run_unzip_s3_zip_to_local(&client, options).await?)
            } else {
                UnzipCommandReport::Local(unzip_s3_zip_to_local(&client, options).await?)
            }
        }
        (ZipSource::Local(source_zip), TreeDestination::Local(destination_dir)) => {
            let mut options = LocalUnzipOptions::new(source_zip, destination_dir);
            options.concurrency = concurrency;
            options.collect_operations = !matches!(report_destination, ReportDestination::None);
            options.ignore_embedded_catalog = ignore_catalog;
            if dry_run {
                UnzipCommandReport::DryRun(dry_run_unzip_file_to_local(options).await?)
            } else {
                UnzipCommandReport::Local(unzip_file_to_local(options).await?)
            }
        }
    };
    activity.finish().await;
    report.write(&report_destination).await?;
    output.write(&unzip_transcript(&report, &report_destination))?;

    if report.has_errors() {
        let message = if dry_run {
            "unzip dry run completed with per-entry errors; rerun with --report for details"
        } else {
            "unzip completed with per-entry errors; rerun with --report for details"
        };
        return Err(message.into());
    }

    Ok(())
}

async fn run_zip(matches: &ArgMatches, output: &Output) -> Result<(), Box<dyn std::error::Error>> {
    let source = matches
        .get_one::<String>("source")
        .expect("required by clap");
    let destination = matches
        .get_one::<String>("destination")
        .expect("required by clap");
    let dry_run = matches.get_flag("dry-run");
    let include_catalog = !matches.get_flag("no-catalog");
    let compression = parse_zip_compression(matches)?;
    let report_destination = ReportDestination::from_cli_value(matches.get_one::<String>("report"));
    let source = parse_tree_source(source)?;
    let destination = parse_zip_destination(destination)?;

    output.write(&Transcript::running(
        if dry_run { "Zip dry run" } else { "Zip" },
        vec![
            format!("{} -> {}", source.display(), destination.display()),
            format!(
                "catalog {}, compression {}{}",
                if include_catalog {
                    "enabled"
                } else {
                    "disabled"
                },
                compression.as_str(),
                if dry_run { ", no changes" } else { "" }
            ),
        ],
    ))?;

    let progress_detail = ActivityDetail::default();
    let progress_sink = progress_detail.clone();
    let progress = UploadProgressHandler::new(move |progress| {
        progress_sink.set(upload_progress_state(&progress));
    });
    let activity = output.start_activity(
        if dry_run { "Planning zip" } else { "Zipping" },
        (!dry_run).then_some(progress_detail),
    );
    let zip_started = Instant::now();
    let report = match (source, destination) {
        (TreeSource::Local(source_dir), ZipDestination::S3(destination)) => {
            let mut options = UploadOptions::new(source_dir, destination);
            options.include_catalog = include_catalog;
            options.compression = compression;
            if dry_run {
                ZipCommandReport::DryRun(dry_run_upload_directory_zip_to_s3(options).await?)
            } else {
                let client = s3_client().await;
                options.progress = Some(progress);
                ZipCommandReport::Upload(upload_directory_zip_to_s3(&client, options).await?)
            }
        }
        (TreeSource::Local(source_dir), ZipDestination::Local(destination_zip)) => {
            let mut options = LocalZipOptions::new(source_dir, destination_zip);
            options.include_catalog = include_catalog;
            options.compression = compression;
            if dry_run {
                ZipCommandReport::DryRun(dry_run_zip_directory_to_file(options).await?)
            } else {
                options.progress = Some(progress);
                ZipCommandReport::Local(zip_directory_to_file(options).await?)
            }
        }
        (TreeSource::S3(source), ZipDestination::S3(destination)) => {
            let client = s3_client().await;
            let mut options = S3PrefixUploadOptions::new(source, destination);
            options.include_catalog = include_catalog;
            options.compression = compression;
            if dry_run {
                ZipCommandReport::DryRun(dry_run_zip_s3_prefix_to_s3(&client, options).await?)
            } else {
                options.progress = Some(progress);
                ZipCommandReport::S3Prefix(zip_s3_prefix_to_s3(&client, options).await?)
            }
        }
        (TreeSource::S3(source), ZipDestination::Local(destination_zip)) => {
            let client = s3_client().await;
            let mut options = S3PrefixLocalZipOptions::new(source, destination_zip);
            options.include_catalog = include_catalog;
            options.compression = compression;
            if dry_run {
                ZipCommandReport::DryRun(dry_run_zip_s3_prefix_to_file(&client, options).await?)
            } else {
                options.progress = Some(progress);
                ZipCommandReport::Local(zip_s3_prefix_to_file(&client, options).await?)
            }
        }
    };
    activity.finish().await;
    let zip_elapsed = zip_started.elapsed();
    report.write(&report_destination).await?;
    output.write(&zip_transcript(&report, &report_destination, zip_elapsed))?;

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

enum TreeSource {
    Local(PathBuf),
    S3(S3Prefix),
}

impl TreeSource {
    fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::S3(prefix) => prefix.uri(),
        }
    }
}

enum TreeDestination {
    Local(PathBuf),
    S3(S3Prefix),
}

impl TreeDestination {
    fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::S3(prefix) => prefix.uri(),
        }
    }
}

enum ZipSource {
    Local(PathBuf),
    S3(S3Object),
}

impl ZipSource {
    fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::S3(object) => object.uri(),
        }
    }
}

enum ZipDestination {
    Local(PathBuf),
    S3(S3Object),
}

impl ZipDestination {
    fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::S3(object) => object.uri(),
        }
    }
}

fn parse_tree_source(value: &str) -> Result<TreeSource, Box<dyn std::error::Error>> {
    reject_file_uri(value)?;
    if value.starts_with("s3://") {
        Ok(TreeSource::S3(S3Prefix::parse(value)?))
    } else {
        Ok(TreeSource::Local(PathBuf::from(value)))
    }
}

fn parse_tree_destination(value: &str) -> Result<TreeDestination, Box<dyn std::error::Error>> {
    reject_file_uri(value)?;
    if value.starts_with("s3://") {
        Ok(TreeDestination::S3(S3Prefix::parse(value)?))
    } else {
        Ok(TreeDestination::Local(PathBuf::from(value)))
    }
}

fn parse_zip_source(value: &str) -> Result<ZipSource, Box<dyn std::error::Error>> {
    reject_file_uri(value)?;
    if value.starts_with("s3://") {
        Ok(ZipSource::S3(S3Object::parse(value)?))
    } else {
        Ok(ZipSource::Local(PathBuf::from(value)))
    }
}

fn parse_zip_destination(value: &str) -> Result<ZipDestination, Box<dyn std::error::Error>> {
    reject_file_uri(value)?;
    if value.starts_with("s3://") {
        Ok(ZipDestination::S3(S3Object::parse(value)?))
    } else {
        Ok(ZipDestination::Local(PathBuf::from(value)))
    }
}

fn validate_delete_extra_destination(
    delete_extra: bool,
    destination: &TreeDestination,
) -> Result<(), Box<dyn std::error::Error>> {
    if !delete_extra {
        return Ok(());
    }

    match destination {
        TreeDestination::Local(_) => {
            Err("--delete-extra is only supported for s3:// destinations".into())
        }
        TreeDestination::S3(prefix) if prefix.prefix.is_empty() => {
            Err("--delete-extra requires a non-empty s3:// destination prefix".into())
        }
        TreeDestination::S3(_) => Ok(()),
    }
}

fn validate_diagnostics_source(
    diagnostics: bool,
    source: &ZipSource,
) -> Result<(), Box<dyn std::error::Error>> {
    if diagnostics && matches!(source, ZipSource::Local(_)) {
        Err("--diagnostics is only supported for s3:// ZIP sources".into())
    } else {
        Ok(())
    }
}

fn reject_file_uri(value: &str) -> Result<(), Box<dyn std::error::Error>> {
    if value.starts_with("file://") {
        Err("file:// URIs are not supported; use a plain local path".into())
    } else {
        Ok(())
    }
}

fn cli() -> Command {
    Command::new("s3-unspool")
        .about("Zip and unzip local paths and S3 prefixes")
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
            Command::new("unzip")
                .about("Unzip a local or S3 ZIP into a local directory or S3 prefix")
                .arg(
                    Arg::new("source")
                        .value_name("SOURCE_ZIP")
                        .help("Source ZIP file, for example ./archive.zip or s3://bucket/archive.zip")
                        .required(true),
                )
                .arg(
                    Arg::new("destination")
                        .value_name("DESTINATION_TREE")
                        .help("Destination tree, for example ./site or s3://bucket/prefix/")
                        .required(true),
                )
                .arg(
                    Arg::new("delete-extra")
                        .long("delete-extra")
                        .help("Delete destination objects under the prefix that are not present in the ZIP")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .help("Inspect the ZIP and destination, then report planned changes without writing or deleting anything")
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
                        .help("Collect source range diagnostics for s3:// ZIP sources in the JSON report")
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
            Command::new("zip")
                .about("Zip a local directory or S3 prefix into a local or S3 ZIP")
                .arg(
                    Arg::new("source")
                        .value_name("SOURCE_TREE")
                        .help("Source tree, for example ./site or s3://bucket/prefix/")
                        .required(true),
                )
                .arg(
                    Arg::new("destination")
                        .value_name("DESTINATION_ZIP")
                        .help("Destination ZIP file, for example ./archive.zip or s3://bucket/archive.zip")
                        .required(true),
                )
                .arg(
                    Arg::new("report")
                        .long("report")
                        .value_name("PATH_OR_-")
                        .num_args(0..=1)
                        .require_equals(true)
                        .default_missing_value("-")
                        .help("Show a formatted zip report, or write JSON to a path with --report=PATH"),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .help("Inspect the source tree, then report the planned archive without writing anything")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("no-catalog")
                        .long("no-catalog")
                        .help("Do not include the embedded MD5 catalog in the ZIP")
                        .action(ArgAction::SetTrue),
                )
                .arg(
                    Arg::new("compression")
                        .long("compression")
                        .value_name("METHOD")
                        .help("Compression method for regular file entries")
                        .value_parser(["deflate", "zstd"])
                        .default_value("deflate"),
                ),
        )
}

fn parse_zip_compression(
    matches: &ArgMatches,
) -> Result<ZipCompression, Box<dyn std::error::Error>> {
    match matches
        .get_one::<String>("compression")
        .map(String::as_str)
        .unwrap_or("deflate")
    {
        "deflate" => Ok(ZipCompression::Deflate),
        "zstd" => {
            #[cfg(feature = "zstd")]
            {
                Ok(ZipCompression::Zstd)
            }
            #[cfg(not(feature = "zstd"))]
            {
                Err("zstd compression requires the s3-unspool-cli `zstd` feature".into())
            }
        }
        other => Err(format!("unsupported ZIP compression method {other:?}").into()),
    }
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

enum ZipCommandReport {
    Upload(UploadReport),
    S3Prefix(S3PrefixUploadReport),
    Local(LocalZipReport),
    DryRun(ZipDryRunReport),
}

impl ZipCommandReport {
    async fn write(
        &self,
        destination: &ReportDestination,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Upload(report) => write_report(destination, report).await,
            Self::S3Prefix(report) => write_report(destination, report).await,
            Self::Local(report) => write_report(destination, report).await,
            Self::DryRun(report) => write_report(destination, report).await,
        }
    }

    fn source(&self) -> String {
        match self {
            Self::Upload(report) => report.source_dir.clone(),
            Self::S3Prefix(report) => report.source.uri(),
            Self::Local(report) => report.source.clone(),
            Self::DryRun(report) => report.source.clone(),
        }
    }

    fn destination(&self) -> String {
        match self {
            Self::Upload(report) => report.destination.uri(),
            Self::S3Prefix(report) => report.destination.uri(),
            Self::Local(report) => report.destination_zip.clone(),
            Self::DryRun(report) => report.destination.clone(),
        }
    }

    fn files(&self) -> usize {
        match self {
            Self::Upload(report) => report.files,
            Self::S3Prefix(report) => report.files,
            Self::Local(report) => report.files,
            Self::DryRun(report) => report.files,
        }
    }

    fn directories(&self) -> usize {
        match self {
            Self::Upload(report) => report.directories,
            Self::S3Prefix(report) => report.directories,
            Self::Local(report) => report.directories,
            Self::DryRun(report) => report.directories,
        }
    }

    fn uncompressed_bytes(&self) -> u64 {
        match self {
            Self::Upload(report) => report.uncompressed_bytes,
            Self::S3Prefix(report) => report.uncompressed_bytes,
            Self::Local(report) => report.uncompressed_bytes,
            Self::DryRun(report) => report.uncompressed_bytes,
        }
    }

    fn zip_bytes(&self) -> Option<u64> {
        match self {
            Self::Upload(report) => Some(report.zip_bytes),
            Self::S3Prefix(report) => Some(report.zip_bytes),
            Self::Local(report) => Some(report.zip_bytes),
            Self::DryRun(_) => None,
        }
    }

    fn include_catalog(&self) -> Option<bool> {
        match self {
            Self::DryRun(report) => Some(report.include_catalog),
            Self::Upload(_) | Self::S3Prefix(_) | Self::Local(_) => None,
        }
    }
}

enum UnzipCommandReport {
    S3(SyncReport),
    LocalZipToS3(LocalZipToS3Report),
    Local(LocalUnzipReport),
    DryRun(UnzipDryRunReport),
}

impl UnzipCommandReport {
    async fn write(
        &self,
        destination: &ReportDestination,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::S3(report) => write_report(destination, report).await,
            Self::LocalZipToS3(report) => write_report(destination, report).await,
            Self::Local(report) => write_report(destination, report).await,
            Self::DryRun(report) => write_report(destination, report).await,
        }
    }

    fn has_errors(&self) -> bool {
        match self {
            Self::S3(report) => report.has_errors(),
            Self::LocalZipToS3(report) => report.has_errors(),
            Self::Local(report) => report.has_errors(),
            Self::DryRun(report) => report.has_errors(),
        }
    }

    fn summary(&self) -> Option<&SyncSummary> {
        match self {
            Self::S3(report) => Some(&report.summary),
            Self::LocalZipToS3(report) => Some(&report.summary),
            Self::Local(report) => Some(&report.summary),
            Self::DryRun(_) => None,
        }
    }

    fn dry_run_summary(&self) -> Option<&UnzipDryRunSummary> {
        match self {
            Self::DryRun(report) => Some(&report.summary),
            Self::S3(_) | Self::LocalZipToS3(_) | Self::Local(_) => None,
        }
    }

    fn source(&self) -> String {
        match self {
            Self::S3(report) => report.source.uri(),
            Self::LocalZipToS3(report) => report.source_zip.clone(),
            Self::Local(report) => report.source_zip.clone(),
            Self::DryRun(report) => report.source_zip.clone(),
        }
    }

    fn destination(&self) -> String {
        match self {
            Self::S3(report) => report.destination.uri(),
            Self::LocalZipToS3(report) => report.destination.uri(),
            Self::Local(report) => report.destination_dir.clone(),
            Self::DryRun(report) => report.destination.clone(),
        }
    }

    fn operations(&self) -> &[ObjectReport] {
        match self {
            Self::S3(report) => &report.operations,
            Self::LocalZipToS3(report) => &report.operations,
            Self::Local(report) => &report.operations,
            Self::DryRun(_) => &[],
        }
    }

    fn dry_run_operations(&self) -> &[DryRunObjectReport] {
        match self {
            Self::DryRun(report) => &report.operations,
            Self::S3(_) | Self::LocalZipToS3(_) | Self::Local(_) => &[],
        }
    }

    fn diagnostics_line(&self) -> Option<String> {
        match self {
            Self::S3(report) => report.diagnostics.as_ref().map(diagnostics_line),
            Self::Local(report) => report.diagnostics.as_ref().map(|diagnostics| {
                format!(
                    "Source: {} GET attempts, {} blocks fetched, {} waits, {:.2}x amplification",
                    diagnostics.source.source_get_attempts,
                    diagnostics.source.fetched_blocks,
                    diagnostics.source.block_waits,
                    diagnostics.source.source_amplification
                )
            }),
            Self::LocalZipToS3(_) => None,
            Self::DryRun(report) => report.diagnostics.as_ref().map(|diagnostics| {
                format!(
                    "Source: {} GET attempts, {} blocks fetched, {} waits, {:.2}x amplification",
                    diagnostics.source.source_get_attempts,
                    diagnostics.source.fetched_blocks,
                    diagnostics.source.block_waits,
                    diagnostics.source.source_amplification
                )
            }),
        }
    }
}

fn zip_transcript(
    report: &ZipCommandReport,
    report_destination: &ReportDestination,
    zip_elapsed: Duration,
) -> Transcript {
    let mut counts = vec![plural(report.files(), "file", "files")];
    let directories = report.directories();
    if directories > 0 {
        counts.push(plural(directories, "directory", "directories"));
    }
    let mut details = if let Some(zip_bytes) = report.zip_bytes() {
        vec![
            format!(
                "{}, {} uncompressed, {} ZIP",
                counts.join(", "),
                format_bytes(report.uncompressed_bytes()),
                format_bytes(zip_bytes)
            ),
            report.destination(),
        ]
    } else {
        let catalog = if report.include_catalog().unwrap_or(true) {
            "catalog included"
        } else {
            "catalog disabled"
        };
        vec![
            format!(
                "{}, {} uncompressed, {catalog}",
                counts.join(", "),
                format_bytes(report.uncompressed_bytes())
            ),
            report.destination(),
        ]
    };
    match report_destination {
        ReportDestination::None => {}
        ReportDestination::JsonFile(path) => details.push(format!("Report: {path}")),
        ReportDestination::Human => details.extend(zip_report_details(report, zip_elapsed)),
    }

    let title = if matches!(report, ZipCommandReport::DryRun(_)) {
        "Zip dry run complete"
    } else {
        "Zip complete"
    };
    Transcript::success(title, details)
}

fn unzip_transcript(
    report: &UnzipCommandReport,
    report_destination: &ReportDestination,
) -> Transcript {
    if let Some(summary) = report.dry_run_summary() {
        return unzip_dry_run_transcript(report, summary, report_destination);
    }
    let summary = report.summary().expect("non-dry-run report has summary");
    let mut details = if summary.uploaded_new == 0
        && summary.uploaded_changed == 0
        && summary.deleted_extra == 0
        && summary.conditional_conflicts == 0
        && summary.errors == 0
    {
        vec![format!(
            "{} unchanged",
            plural(summary.skipped_unchanged, "entry", "entries")
        )]
    } else {
        vec![format!(
            "{}: {} new, {} changed, {} unchanged",
            plural(summary.zip_files, "entry", "entries"),
            summary.uploaded_new,
            summary.uploaded_changed,
            summary.skipped_unchanged
        )]
    };

    if summary.destination_objects > 0 {
        details.push(format!(
            "Destination: {} listed",
            plural(summary.destination_objects, "object", "objects")
        ));
    }
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
            plural(summary.errors, "entry", "entries")
        ));
    }
    if let Some(line) = report.diagnostics_line() {
        details.push(line);
    }
    match report_destination {
        ReportDestination::None => {}
        ReportDestination::JsonFile(path) => details.push(format!("Report: {path}")),
        ReportDestination::Human => details.extend(unzip_report_details(report)),
    }

    if summary.errors > 0 {
        Transcript::error("Unzip completed with errors", details)
    } else if summary.conditional_conflicts > 0 {
        Transcript::notice("Unzip completed with conflicts", details)
    } else if summary.uploaded_new == 0
        && summary.uploaded_changed == 0
        && summary.deleted_extra == 0
    {
        Transcript::success("Up to date", details)
    } else {
        Transcript::success("Unzip complete", details)
    }
}

fn unzip_dry_run_transcript(
    report: &UnzipCommandReport,
    summary: &UnzipDryRunSummary,
    report_destination: &ReportDestination,
) -> Transcript {
    let mut details = if summary.would_upload_new == 0
        && summary.would_upload_changed == 0
        && summary.would_delete_extra == 0
        && summary.errors == 0
    {
        vec![format!(
            "{} unchanged",
            plural(summary.skipped_unchanged, "entry", "entries")
        )]
    } else {
        vec![format!(
            "{}: {} would create, {} would replace, {} unchanged",
            plural(summary.zip_files, "entry", "entries"),
            summary.would_upload_new,
            summary.would_upload_changed,
            summary.skipped_unchanged
        )]
    };

    if summary.destination_objects > 0 {
        details.push(format!(
            "Destination: {} listed",
            plural(summary.destination_objects, "object", "objects")
        ));
    }
    if summary.would_delete_extra > 0 {
        details.push(format!(
            "Would delete: {} extra",
            plural(summary.would_delete_extra, "object", "objects")
        ));
    }
    if summary.errors > 0 {
        details.push(format!(
            "Errors: {}",
            plural(summary.errors, "entry", "entries")
        ));
    }
    if let Some(line) = report.diagnostics_line() {
        details.push(line);
    }
    match report_destination {
        ReportDestination::None => {}
        ReportDestination::JsonFile(path) => details.push(format!("Report: {path}")),
        ReportDestination::Human => details.extend(unzip_report_details(report)),
    }

    if summary.errors > 0 {
        Transcript::error("Unzip dry run completed with errors", details)
    } else {
        Transcript::success("Unzip dry run complete", details)
    }
}

fn zip_report_details(report: &ZipCommandReport, zip_elapsed: Duration) -> Vec<String> {
    let mut details = vec![
        "Report:".to_string(),
        format!("  Source: {}", report.source()),
        format!("  Destination: {}", report.destination()),
        format!("  Files: {}", plural(report.files(), "file", "files")),
        format!(
            "  Directories: {}",
            plural(report.directories(), "directory", "directories")
        ),
        format!(
            "  Uncompressed: {}",
            format_bytes(report.uncompressed_bytes())
        ),
    ];
    if let Some(include_catalog) = report.include_catalog() {
        details.push(format!(
            "  Catalog: {}",
            if include_catalog {
                "included"
            } else {
                "disabled"
            }
        ));
    }
    if let Some(zip_bytes) = report.zip_bytes() {
        details.push(format!("  ZIP: {}", format_bytes(zip_bytes)));
        details.push(format!("  Wall time: {}", format_elapsed(zip_elapsed)));
        details.push(format!(
            "  Zip speed: {}",
            format_upload_speed(zip_bytes, zip_elapsed)
        ));
    }
    details
}

fn unzip_report_details(report: &UnzipCommandReport) -> Vec<String> {
    if let Some(summary) = report.dry_run_summary() {
        return unzip_dry_run_report_details(report, summary);
    }
    let summary = report.summary().expect("non-dry-run report has summary");
    let mut details = vec![
        "Report:".to_string(),
        format!("  Source: {}", report.source()),
        format!("  Destination: {}", report.destination()),
        format!(
            "  ZIP entries: {}",
            plural(summary.zip_files, "entry", "entries")
        ),
        format!(
            "  Operations: {} new, {} changed, {} unchanged, {} deleted",
            summary.uploaded_new,
            summary.uploaded_changed,
            summary.skipped_unchanged,
            summary.deleted_extra
        ),
    ];

    if summary.destination_objects > 0 {
        details.push(format!(
            "  Destination listed: {}",
            plural(summary.destination_objects, "object", "objects")
        ));
    }
    if summary.conditional_conflicts > 0 || summary.errors > 0 {
        details.push(format!(
            "  Issues: {} conflicts, {} errors",
            summary.conditional_conflicts, summary.errors
        ));
    }
    if let Some(line) = report.diagnostics_line() {
        details.push(format!("  {line}"));
    }

    details.extend(operation_report_details(report.operations()));
    details
}

fn unzip_dry_run_report_details(
    report: &UnzipCommandReport,
    summary: &UnzipDryRunSummary,
) -> Vec<String> {
    let mut details = vec![
        "Report:".to_string(),
        format!("  Source: {}", report.source()),
        format!("  Destination: {}", report.destination()),
        format!(
            "  ZIP entries: {}",
            plural(summary.zip_files, "entry", "entries")
        ),
        format!(
            "  Operations: {} would create, {} would replace, {} unchanged, {} would delete",
            summary.would_upload_new,
            summary.would_upload_changed,
            summary.skipped_unchanged,
            summary.would_delete_extra
        ),
    ];

    if summary.destination_objects > 0 {
        details.push(format!(
            "  Destination listed: {}",
            plural(summary.destination_objects, "object", "objects")
        ));
    }
    if summary.errors > 0 {
        details.push(format!("  Issues: {} errors", summary.errors));
    }
    if let Some(line) = report.diagnostics_line() {
        details.push(format!("  {line}"));
    }

    details.extend(dry_run_operation_report_details(
        report.dry_run_operations(),
    ));
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

fn dry_run_operation_report_details(operations: &[DryRunObjectReport]) -> Vec<String> {
    let noteworthy = operations
        .iter()
        .filter(|operation| operation.status != DryRunOperationStatus::SkippedUnchanged)
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
            .map(|operation| format!("    {}", dry_run_operation_report_line(operation))),
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

fn dry_run_operation_report_line(operation: &DryRunObjectReport) -> String {
    let status = match operation.status {
        DryRunOperationStatus::WouldUploadNew => "would create",
        DryRunOperationStatus::WouldUploadChanged => "would replace",
        DryRunOperationStatus::SkippedUnchanged => "unchanged",
        DryRunOperationStatus::WouldDeleteExtra => "would delete extra",
        DryRunOperationStatus::Error => "error",
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
    fn parses_unzip_subcommand() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "--quiet",
                "--color",
                "never",
                "unzip",
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

        let Some(("unzip", extract)) = matches.subcommand() else {
            panic!("expected unzip subcommand");
        };
        assert!(extract.get_flag("delete-extra"));
        assert!(extract.get_flag("ignore-catalog"));
        assert_eq!(
            extract.get_one::<String>("source").map(String::as_str),
            Some("s3://source-bucket/archive.zip")
        );
    }

    #[test]
    fn parses_zip_subcommand() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "zip",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("zip", upload)) = matches.subcommand() else {
            panic!("expected zip subcommand");
        };
        assert_eq!(
            upload.get_one::<String>("source").map(String::as_str),
            Some("/tmp/site")
        );
        assert_eq!(
            upload.get_one::<String>("destination").map(String::as_str),
            Some("s3://destination-bucket/site.zip")
        );
    }

    #[test]
    fn parses_zip_dry_run_and_no_catalog() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "zip",
                "--dry-run",
                "--no-catalog",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("zip", upload)) = matches.subcommand() else {
            panic!("expected zip subcommand");
        };
        assert!(upload.get_flag("dry-run"));
        assert!(upload.get_flag("no-catalog"));
    }

    #[test]
    fn parses_zip_compression() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "zip",
                "--compression",
                "zstd",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("zip", upload)) = matches.subcommand() else {
            panic!("expected zip subcommand");
        };
        assert_eq!(
            upload.get_one::<String>("compression").map(String::as_str),
            Some("zstd")
        );
        #[cfg(feature = "zstd")]
        assert_eq!(parse_zip_compression(upload).unwrap(), ZipCompression::Zstd);
        #[cfg(not(feature = "zstd"))]
        assert!(
            parse_zip_compression(upload)
                .unwrap_err()
                .to_string()
                .contains("requires the s3-unspool-cli `zstd` feature")
        );
    }

    #[test]
    fn parses_unzip_dry_run() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "unzip",
                "--dry-run",
                "--delete-extra",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        let Some(("unzip", extract)) = matches.subcommand() else {
            panic!("expected unzip subcommand");
        };
        assert!(extract.get_flag("dry-run"));
        assert!(extract.get_flag("delete-extra"));
    }

    #[test]
    fn parses_all_zip_endpoint_combinations() {
        for (source, destination) in [
            ("/tmp/site", "/tmp/site.zip"),
            ("/tmp/site", "s3://bucket/site.zip"),
            ("s3://bucket/site/", "/tmp/site.zip"),
            ("s3://bucket/site/", "s3://bucket/site.zip"),
        ] {
            let matches = cli()
                .try_get_matches_from(["s3-unspool", "zip", source, destination])
                .unwrap();
            let Some(("zip", zip)) = matches.subcommand() else {
                panic!("expected zip subcommand");
            };
            assert_eq!(
                zip.get_one::<String>("source").map(String::as_str),
                Some(source)
            );
            assert_eq!(
                zip.get_one::<String>("destination").map(String::as_str),
                Some(destination)
            );
        }
    }

    #[test]
    fn parses_all_unzip_endpoint_combinations() {
        for (source, destination) in [
            ("/tmp/site.zip", "/tmp/site"),
            ("/tmp/site.zip", "s3://bucket/site/"),
            ("s3://bucket/site.zip", "/tmp/site"),
            ("s3://bucket/site.zip", "s3://bucket/site/"),
        ] {
            let matches = cli()
                .try_get_matches_from(["s3-unspool", "unzip", source, destination])
                .unwrap();
            let Some(("unzip", unzip)) = matches.subcommand() else {
                panic!("expected unzip subcommand");
            };
            assert_eq!(
                unzip.get_one::<String>("source").map(String::as_str),
                Some(source)
            );
            assert_eq!(
                unzip.get_one::<String>("destination").map(String::as_str),
                Some(destination)
            );
        }
    }

    #[test]
    fn rejects_file_uri_endpoint_values() {
        assert!(parse_tree_source("file:///tmp/site").is_err());
        assert!(parse_zip_source("file:///tmp/site.zip").is_err());
        assert!(parse_tree_destination("file:///tmp/site").is_err());
        assert!(parse_zip_destination("file:///tmp/site.zip").is_err());
    }

    #[test]
    fn rejects_delete_extra_for_bucket_root_destination() {
        let destination = parse_tree_destination("s3://bucket").unwrap();

        let err = validate_delete_extra_destination(true, &destination).unwrap_err();

        assert!(err.to_string().contains("non-empty"));
    }

    #[test]
    fn rejects_delete_extra_for_local_destination() {
        let destination = parse_tree_destination("/tmp/site").unwrap();

        let err = validate_delete_extra_destination(true, &destination).unwrap_err();

        assert!(err.to_string().contains("s3://"));
    }

    #[test]
    fn rejects_diagnostics_for_local_zip_sources() {
        let source = parse_zip_source("/tmp/site.zip").unwrap();

        let err = validate_diagnostics_source(true, &source).unwrap_err();

        assert!(err.to_string().contains("s3:// ZIP sources"));
        let source = parse_zip_source("s3://bucket/site.zip").unwrap();
        validate_diagnostics_source(true, &source).unwrap();
    }

    #[test]
    fn parses_bare_report_as_stdout_for_unzip() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "unzip",
                "--report",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        let Some(("unzip", extract)) = matches.subcommand() else {
            panic!("expected unzip subcommand");
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
    fn parses_report_path_for_unzip() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "unzip",
                "--report=report.json",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        let Some(("unzip", extract)) = matches.subcommand() else {
            panic!("expected unzip subcommand");
        };
        assert_eq!(
            extract.get_one::<String>("report").map(String::as_str),
            Some("report.json")
        );
    }

    #[test]
    fn parses_report_path_after_unzip_positionals() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "unzip",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
                "--report=report.json",
            ])
            .unwrap();

        let Some(("unzip", extract)) = matches.subcommand() else {
            panic!("expected unzip subcommand");
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
                "unzip",
                "--report",
                "--concurrency=32",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        let Some(("unzip", extract)) = matches.subcommand() else {
            panic!("expected unzip subcommand");
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
    fn parses_bare_report_as_stdout_for_zip() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "zip",
                "--report",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("zip", upload)) = matches.subcommand() else {
            panic!("expected zip subcommand");
        };
        assert_eq!(
            upload.get_one::<String>("report").map(String::as_str),
            Some("-")
        );
        assert_eq!(
            upload.get_one::<String>("source").map(String::as_str),
            Some("/tmp/site")
        );
    }

    #[test]
    fn parses_report_dash_as_stdout_for_zip() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "zip",
                "--report=-",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("zip", upload)) = matches.subcommand() else {
            panic!("expected zip subcommand");
        };
        assert_eq!(
            upload.get_one::<String>("report").map(String::as_str),
            Some("-")
        );
    }

    #[test]
    fn parses_report_path_for_zip() {
        let matches = cli()
            .try_get_matches_from([
                "s3-unspool",
                "zip",
                "--report=report.json",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap();

        let Some(("zip", upload)) = matches.subcommand() else {
            panic!("expected zip subcommand");
        };
        assert_eq!(
            upload.get_one::<String>("report").map(String::as_str),
            Some("report.json")
        );
    }

    #[test]
    fn renders_zip_transcript() {
        let report = UploadReport {
            source_dir: "./site".to_string(),
            destination: S3Object::parse("s3://bucket/site.zip").unwrap(),
            files: 2,
            directories: 0,
            uncompressed_bytes: 1536,
            zip_bytes: 768,
        };

        let transcript = zip_transcript(
            &ZipCommandReport::Upload(report),
            &ReportDestination::JsonFile("report.json".to_string()),
            Duration::from_secs(2),
        );

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Zip complete\n  └ 2 files, 1.5 KiB uncompressed, 768 B ZIP\n    s3://bucket/site.zip\n    Report: report.json"
        );
    }

    #[test]
    fn renders_zip_transcript_with_human_report() {
        let report = UploadReport {
            source_dir: "./site".to_string(),
            destination: S3Object::parse("s3://bucket/site.zip").unwrap(),
            files: 2,
            directories: 1,
            uncompressed_bytes: 4 * 1024 * 1024,
            zip_bytes: 3 * 1024 * 1024,
        };

        let transcript = zip_transcript(
            &ZipCommandReport::Upload(report),
            &ReportDestination::Human,
            Duration::from_secs(2),
        );

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Zip complete\n  └ 2 files, 1 directory, 4.0 MiB uncompressed, 3.0 MiB ZIP\n    s3://bucket/site.zip\n    Report:\n      Source: ./site\n      Destination: s3://bucket/site.zip\n      Files: 2 files\n      Directories: 1 directory\n      Uncompressed: 4.0 MiB\n      ZIP: 3.0 MiB\n      Wall time: 00:02\n      Zip speed: 1.50 MiB/s"
        );
    }

    #[test]
    fn renders_zip_dry_run_transcript() {
        let report = ZipDryRunReport {
            source: "./site".to_string(),
            destination: "s3://bucket/site.zip".to_string(),
            files: 2,
            directories: 1,
            entries: 3,
            uncompressed_bytes: 4 * 1024 * 1024,
            include_catalog: false,
        };

        let transcript = zip_transcript(
            &ZipCommandReport::DryRun(report),
            &ReportDestination::Human,
            Duration::from_secs(0),
        );

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Zip dry run complete\n  └ 2 files, 1 directory, 4.0 MiB uncompressed, catalog disabled\n    s3://bucket/site.zip\n    Report:\n      Source: ./site\n      Destination: s3://bucket/site.zip\n      Files: 2 files\n      Directories: 1 directory\n      Uncompressed: 4.0 MiB\n      Catalog: disabled"
        );
    }

    #[test]
    fn renders_up_to_date_unzip_transcript() {
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

        let transcript =
            unzip_transcript(&UnzipCommandReport::S3(report), &ReportDestination::None);

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Up to date\n  └ 10 entries unchanged\n    Destination: 10 objects listed"
        );
    }

    #[test]
    fn renders_changed_unzip_transcript_with_diagnostics() {
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

        let transcript = unzip_transcript(
            &UnzipCommandReport::S3(report),
            &ReportDestination::JsonFile("report.json".to_string()),
        );

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Unzip complete\n  └ 10 entries: 1 new, 2 changed, 7 unchanged\n    Destination: 12 objects listed\n    Deleted: 1 object extra\n    Source: 3 GET attempts, 2 blocks fetched, 1 waits, 1.25x amplification\n    Report: report.json"
        );
    }

    #[test]
    fn renders_unzip_dry_run_transcript_with_human_report() {
        let report = UnzipDryRunReport {
            source_zip: "s3://bucket/site.zip".to_string(),
            destination: "s3://bucket/www/".to_string(),
            summary: UnzipDryRunSummary {
                zip_files: 3,
                destination_objects: 4,
                would_upload_new: 1,
                would_upload_changed: 1,
                skipped_unchanged: 1,
                would_delete_extra: 1,
                errors: 0,
            },
            diagnostics: None,
            operations: vec![
                DryRunObjectReport {
                    status: DryRunOperationStatus::WouldUploadNew,
                    key: "www/a.txt".to_string(),
                    zip_path: Some("a.txt".to_string()),
                    size: Some(10),
                    md5: None,
                    destination_etag: None,
                    message: None,
                },
                DryRunObjectReport {
                    status: DryRunOperationStatus::WouldUploadChanged,
                    key: "www/b.txt".to_string(),
                    zip_path: Some("b.txt".to_string()),
                    size: Some(20),
                    md5: None,
                    destination_etag: Some("\"etag\"".to_string()),
                    message: None,
                },
                DryRunObjectReport {
                    status: DryRunOperationStatus::SkippedUnchanged,
                    key: "www/c.txt".to_string(),
                    zip_path: Some("c.txt".to_string()),
                    size: Some(30),
                    md5: None,
                    destination_etag: Some("\"same\"".to_string()),
                    message: None,
                },
                DryRunObjectReport {
                    status: DryRunOperationStatus::WouldDeleteExtra,
                    key: "www/old.txt".to_string(),
                    zip_path: None,
                    size: None,
                    md5: None,
                    destination_etag: Some("\"old\"".to_string()),
                    message: None,
                },
            ],
        };

        let transcript = unzip_transcript(
            &UnzipCommandReport::DryRun(report),
            &ReportDestination::Human,
        );

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Unzip dry run complete\n  └ 3 entries: 1 would create, 1 would replace, 1 unchanged\n    Destination: 4 objects listed\n    Would delete: 1 object extra\n    Report:\n      Source: s3://bucket/site.zip\n      Destination: s3://bucket/www/\n      ZIP entries: 3 entries\n      Operations: 1 would create, 1 would replace, 1 unchanged, 1 would delete\n      Destination listed: 4 objects\n      Objects:\n        would create: www/a.txt (10 B)\n        would replace: www/b.txt (20 B)\n        would delete extra: www/old.txt"
        );
    }

    #[test]
    fn renders_unzip_transcript_with_human_report() {
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

        let transcript =
            unzip_transcript(&UnzipCommandReport::S3(report), &ReportDestination::Human);

        assert_eq!(
            transcript.render(Theme { color: false }),
            "✓ Unzip complete\n  └ 3 entries: 1 new, 1 changed, 1 unchanged\n    Destination: 2 objects listed\n    Report:\n      Source: s3://bucket/site.zip\n      Destination: s3://bucket/www/\n      ZIP entries: 3 entries\n      Operations: 1 new, 1 changed, 1 unchanged, 0 deleted\n      Destination listed: 2 objects\n      Objects:\n        uploaded new: www/a.txt (10 B)\n        uploaded changed: www/b.txt (20 B)"
        );
    }

    #[test]
    fn renders_conflict_unzip_transcript() {
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

        let transcript =
            unzip_transcript(&UnzipCommandReport::S3(report), &ReportDestination::None);

        assert_eq!(
            transcript.render(Theme { color: false }),
            "! Unzip completed with conflicts\n  └ 3 entries: 0 new, 1 changed, 1 unchanged\n    Destination: 3 objects listed\n    Conflicts: 1 conditional write"
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
