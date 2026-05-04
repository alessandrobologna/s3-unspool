use std::time::Instant;

use clap::ArgMatches;
use s3_unspool as unspool;

use crate::aws::s3_client;
use crate::cli::parse_zip_compression;
use crate::endpoint::{TreeSource, ZipDestination, parse_tree_source, parse_zip_destination};
use crate::reports::{ReportDestination, ZipCommandReport, zip_transcript};
use crate::ui::{ActivityDetail, Output, Transcript, upload_progress_state};

pub(crate) async fn run_zip(
    matches: &ArgMatches,
    output: &Output,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let progress = unspool::UploadProgressHandler::new(move |progress| {
        progress_sink.set(upload_progress_state(&progress));
    });
    let activity = output.start_activity(
        if dry_run { "Planning zip" } else { "Zipping" },
        (!dry_run).then_some(progress_detail),
    );
    let zip_started = Instant::now();
    let report = match (source, destination) {
        (TreeSource::Local(source_dir), ZipDestination::S3(destination)) => {
            let options =
                unspool::UploadOptions::new(source_dir, destination).with_compression(compression);
            let options = if include_catalog {
                options
            } else {
                options.without_catalog()
            };
            if dry_run {
                ZipCommandReport::DryRun(
                    unspool::dry_run_upload_directory_zip_to_s3(options).await?,
                )
            } else {
                let client = s3_client().await;
                ZipCommandReport::Upload(
                    unspool::upload_directory_zip_to_s3(
                        &client,
                        options.with_progress_handler(progress.clone()),
                    )
                    .await?,
                )
            }
        }
        (TreeSource::Local(source_dir), ZipDestination::Local(destination_zip)) => {
            let options = unspool::LocalZipOptions::new(source_dir, destination_zip)
                .with_compression(compression);
            let options = if include_catalog {
                options
            } else {
                options.without_catalog()
            };
            if dry_run {
                ZipCommandReport::DryRun(unspool::dry_run_zip_directory_to_file(options).await?)
            } else {
                ZipCommandReport::Local(
                    unspool::zip_directory_to_file(options.with_progress_handler(progress.clone()))
                        .await?,
                )
            }
        }
        (TreeSource::S3(source), ZipDestination::S3(destination)) => {
            let client = s3_client().await;
            let options = unspool::S3PrefixUploadOptions::new(source, destination)
                .with_compression(compression);
            let options = if include_catalog {
                options
            } else {
                options.without_catalog()
            };
            if dry_run {
                ZipCommandReport::DryRun(
                    unspool::dry_run_zip_s3_prefix_to_s3(&client, options).await?,
                )
            } else {
                ZipCommandReport::S3Prefix(
                    unspool::zip_s3_prefix_to_s3(
                        &client,
                        options.with_progress_handler(progress.clone()),
                    )
                    .await?,
                )
            }
        }
        (TreeSource::S3(source), ZipDestination::Local(destination_zip)) => {
            let client = s3_client().await;
            let options = unspool::S3PrefixLocalZipOptions::new(source, destination_zip)
                .with_compression(compression);
            let options = if include_catalog {
                options
            } else {
                options.without_catalog()
            };
            if dry_run {
                ZipCommandReport::DryRun(
                    unspool::dry_run_zip_s3_prefix_to_file(&client, options).await?,
                )
            } else {
                ZipCommandReport::Local(
                    unspool::zip_s3_prefix_to_file(
                        &client,
                        options.with_progress_handler(progress.clone()),
                    )
                    .await?,
                )
            }
        }
    };
    activity.finish().await;
    let zip_elapsed = zip_started.elapsed();
    report.write(&report_destination).await?;
    output.write(&zip_transcript(&report, &report_destination, zip_elapsed))?;

    Ok(())
}
