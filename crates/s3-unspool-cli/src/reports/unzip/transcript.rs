use s3_unspool as unspool;

use super::UnzipCommandReport;
use super::details::unzip_report_details;
use crate::reports::ReportDestination;
use crate::ui::{Transcript, plural};

pub(crate) fn unzip_transcript(
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
    summary: &unspool::UnzipDryRunSummary,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reports::{ReportDestination, UnzipCommandReport};
    use crate::ui::Theme;
    use s3_unspool as unspool;

    #[test]
    fn renders_up_to_date_unzip_transcript() {
        let report = unspool::SyncReport {
            source: unspool::S3Object::parse("s3://bucket/site.zip").unwrap(),
            destination: unspool::S3Prefix::parse("s3://bucket/www/").unwrap(),
            summary: unspool::SyncSummary {
                zip_files: 10,
                destination_objects: 10,
                skipped_unchanged: 10,
                ..unspool::SyncSummary::default()
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
        let report = unspool::SyncReport {
            source: unspool::S3Object::parse("s3://bucket/site.zip").unwrap(),
            destination: unspool::S3Prefix::parse("s3://bucket/www/").unwrap(),
            summary: unspool::SyncSummary {
                zip_files: 10,
                destination_objects: 12,
                uploaded_new: 1,
                uploaded_changed: 2,
                skipped_unchanged: 7,
                deleted_extra: 1,
                ..unspool::SyncSummary::default()
            },
            diagnostics: Some(unspool::SyncDiagnostics {
                concurrency: 64,
                put_concurrency: 8,
                put_retry: unspool::PutRetryDiagnostics {
                    max_attempts: 6,
                    base_delay_ms: 250,
                    max_delay_ms: 5_000,
                    slowdown_base_delay_ms: 1_000,
                    slowdown_max_delay_ms: 30_000,
                    jitter: unspool::RetryJitter::Full,
                },
                source_block_size: 8192,
                source_block_merge_gap: 1024,
                source_get_concurrency: 4,
                source_window_capacity: 4096,
                source: unspool::SourceDiagnostics {
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
                put: unspool::PutDiagnostics::default(),
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
        let report = unspool::UnzipDryRunReport {
            source_zip: "s3://bucket/site.zip".to_string(),
            destination: "s3://bucket/www/".to_string(),
            summary: unspool::UnzipDryRunSummary {
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
                unspool::DryRunObjectReport {
                    status: unspool::DryRunOperationStatus::WouldUploadNew,
                    key: "www/a.txt".to_string(),
                    zip_path: Some("a.txt".to_string()),
                    size: Some(10),
                    md5: None,
                    destination_etag: None,
                    message: None,
                },
                unspool::DryRunObjectReport {
                    status: unspool::DryRunOperationStatus::WouldUploadChanged,
                    key: "www/b.txt".to_string(),
                    zip_path: Some("b.txt".to_string()),
                    size: Some(20),
                    md5: None,
                    destination_etag: Some("\"etag\"".to_string()),
                    message: None,
                },
                unspool::DryRunObjectReport {
                    status: unspool::DryRunOperationStatus::SkippedUnchanged,
                    key: "www/c.txt".to_string(),
                    zip_path: Some("c.txt".to_string()),
                    size: Some(30),
                    md5: None,
                    destination_etag: Some("\"same\"".to_string()),
                    message: None,
                },
                unspool::DryRunObjectReport {
                    status: unspool::DryRunOperationStatus::WouldDeleteExtra,
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
        let report = unspool::SyncReport {
            source: unspool::S3Object::parse("s3://bucket/site.zip").unwrap(),
            destination: unspool::S3Prefix::parse("s3://bucket/www/").unwrap(),
            summary: unspool::SyncSummary {
                zip_files: 3,
                destination_objects: 2,
                uploaded_new: 1,
                uploaded_changed: 1,
                skipped_unchanged: 1,
                ..unspool::SyncSummary::default()
            },
            diagnostics: None,
            operations: vec![
                unspool::ObjectReport {
                    status: unspool::OperationStatus::UploadedNew,
                    key: "www/a.txt".to_string(),
                    zip_path: Some("a.txt".to_string()),
                    size: Some(10),
                    md5: None,
                    destination_etag: None,
                    message: None,
                },
                unspool::ObjectReport {
                    status: unspool::OperationStatus::UploadedChanged,
                    key: "www/b.txt".to_string(),
                    zip_path: Some("b.txt".to_string()),
                    size: Some(20),
                    md5: None,
                    destination_etag: Some("old".to_string()),
                    message: None,
                },
                unspool::ObjectReport {
                    status: unspool::OperationStatus::SkippedUnchanged,
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
        let report = unspool::SyncReport {
            source: unspool::S3Object::parse("s3://bucket/site.zip").unwrap(),
            destination: unspool::S3Prefix::parse("s3://bucket/www/").unwrap(),
            summary: unspool::SyncSummary {
                zip_files: 3,
                destination_objects: 3,
                uploaded_changed: 1,
                skipped_unchanged: 1,
                conditional_conflicts: 1,
                ..unspool::SyncSummary::default()
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
}
