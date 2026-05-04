use clap::ArgMatches;
use s3_unspool as unspool;

use crate::aws::s3_client;
use crate::endpoint::{
    TreeDestination, ZipSource, parse_tree_destination, parse_zip_source,
    unzip_selection_from_matches, validate_delete_extra_destination,
    validate_delete_extra_selection, validate_diagnostics_source,
};
use crate::reports::{ReportDestination, UnzipCommandReport, unzip_transcript};
use crate::ui::{Output, Transcript};

pub(crate) async fn run_unzip(
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
    let selection = unzip_selection_from_matches(matches);
    let report_destination = ReportDestination::from_cli_value(matches.get_one::<String>("report"));
    let collect_operations = !matches!(report_destination, ReportDestination::None);
    let source = parse_zip_source(source)?;
    let destination = parse_tree_destination(destination)?;
    validate_delete_extra_destination(delete_extra, &destination)?;
    validate_delete_extra_selection(delete_extra, &selection)?;
    validate_diagnostics_source(diagnostics, &source)?;

    let mut details = vec![
        format!("{} -> {}", source.display(), destination.display()),
        format!(
            "{} workers{}{}",
            concurrency,
            if delete_extra { ", delete extra" } else { "" },
            if dry_run { ", no changes" } else { "" }
        ),
    ];
    if !selection.is_empty() {
        details.push(format!(
            "{} selection pattern filters",
            selection.as_patterns().len()
        ));
    }

    output.write(&Transcript::running(
        if dry_run { "Unzip dry run" } else { "Unzip" },
        details,
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
            let options = unspool::SyncOptions::new(source, destination)
                .with_concurrency(concurrency)
                .with_selection(selection.clone());
            let options = if delete_extra {
                options.delete_extra_objects()
            } else {
                options
            };
            let options = if diagnostics {
                options.collect_diagnostics()
            } else {
                options
            };
            let options = if collect_operations {
                options
            } else {
                options.without_operations()
            };
            let options = if ignore_catalog {
                options.force_hash_comparison()
            } else {
                options
            };
            if dry_run {
                UnzipCommandReport::DryRun(unspool::dry_run_sync_zip_to_s3(&client, options).await?)
            } else {
                UnzipCommandReport::S3(unspool::sync_zip_to_s3(&client, options).await?)
            }
        }
        (ZipSource::Local(source_zip), TreeDestination::S3(destination)) => {
            let client = s3_client().await;
            let options = unspool::LocalZipSyncOptions::new(source_zip, destination)
                .with_concurrency(concurrency)
                .with_selection(selection.clone());
            let options = if delete_extra {
                options.delete_extra_objects()
            } else {
                options
            };
            let options = if collect_operations {
                options
            } else {
                options.without_operations()
            };
            let options = if ignore_catalog {
                options.force_hash_comparison()
            } else {
                options
            };
            if dry_run {
                UnzipCommandReport::DryRun(
                    unspool::dry_run_unzip_file_to_s3(&client, options).await?,
                )
            } else {
                UnzipCommandReport::LocalZipToS3(unspool::unzip_file_to_s3(&client, options).await?)
            }
        }
        (ZipSource::S3(source), TreeDestination::Local(destination_dir)) => {
            let client = s3_client().await;
            let options = unspool::S3ZipLocalUnzipOptions::new(source, destination_dir)
                .with_concurrency(concurrency)
                .with_selection(selection.clone());
            let options = if diagnostics {
                options.collect_diagnostics()
            } else {
                options
            };
            let options = if collect_operations {
                options
            } else {
                options.without_operations()
            };
            let options = if ignore_catalog {
                options.force_hash_comparison()
            } else {
                options
            };
            if dry_run {
                UnzipCommandReport::DryRun(
                    unspool::dry_run_unzip_s3_zip_to_local(&client, options).await?,
                )
            } else {
                UnzipCommandReport::Local(unspool::unzip_s3_zip_to_local(&client, options).await?)
            }
        }
        (ZipSource::Local(source_zip), TreeDestination::Local(destination_dir)) => {
            let options = unspool::LocalUnzipOptions::new(source_zip, destination_dir)
                .with_concurrency(concurrency)
                .with_selection(selection);
            let options = if collect_operations {
                options
            } else {
                options.without_operations()
            };
            let options = if ignore_catalog {
                options.force_hash_comparison()
            } else {
                options
            };
            if dry_run {
                UnzipCommandReport::DryRun(unspool::dry_run_unzip_file_to_local(options).await?)
            } else {
                UnzipCommandReport::Local(unspool::unzip_file_to_local(options).await?)
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
