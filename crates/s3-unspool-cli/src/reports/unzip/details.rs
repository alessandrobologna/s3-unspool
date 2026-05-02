use s3_unspool as unspool;

use super::UnzipCommandReport;
use crate::ui::{format_bytes, plural, truncate_text};

pub(super) fn unzip_report_details(report: &UnzipCommandReport) -> Vec<String> {
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
    summary: &unspool::UnzipDryRunSummary,
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

fn operation_report_details(operations: &[unspool::ObjectReport]) -> Vec<String> {
    let noteworthy = operations
        .iter()
        .filter(|operation| operation.status != unspool::OperationStatus::SkippedUnchanged)
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

fn dry_run_operation_report_details(operations: &[unspool::DryRunObjectReport]) -> Vec<String> {
    let noteworthy = operations
        .iter()
        .filter(|operation| operation.status != unspool::DryRunOperationStatus::SkippedUnchanged)
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

fn operation_report_line(operation: &unspool::ObjectReport) -> String {
    let status = match operation.status {
        unspool::OperationStatus::UploadedNew => "uploaded new",
        unspool::OperationStatus::UploadedChanged => "uploaded changed",
        unspool::OperationStatus::SkippedUnchanged => "unchanged",
        unspool::OperationStatus::ConditionalConflict => "conflict",
        unspool::OperationStatus::DeletedExtra => "deleted extra",
        unspool::OperationStatus::Error => "error",
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

fn dry_run_operation_report_line(operation: &unspool::DryRunObjectReport) -> String {
    let status = match operation.status {
        unspool::DryRunOperationStatus::WouldUploadNew => "would create",
        unspool::DryRunOperationStatus::WouldUploadChanged => "would replace",
        unspool::DryRunOperationStatus::SkippedUnchanged => "unchanged",
        unspool::DryRunOperationStatus::WouldDeleteExtra => "would delete extra",
        unspool::DryRunOperationStatus::Error => "error",
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

pub(super) fn diagnostics_line(diagnostics: &unspool::SyncDiagnostics) -> String {
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

fn format_put_failure_codes(diagnostics: &unspool::PutDiagnostics) -> String {
    diagnostics
        .failures_by_error_code
        .iter()
        .map(|(code, count)| format!("{code}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}
