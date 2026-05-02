use s3_unspool as unspool;

use super::super::{ReportDestination, write_report};
use super::details::diagnostics_line;

pub(crate) enum UnzipCommandReport {
    S3(unspool::SyncReport),
    LocalZipToS3(unspool::LocalZipToS3Report),
    Local(unspool::LocalUnzipReport),
    DryRun(unspool::UnzipDryRunReport),
}

impl UnzipCommandReport {
    pub(crate) async fn write(
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

    pub(crate) fn has_errors(&self) -> bool {
        match self {
            Self::S3(report) => report.has_errors(),
            Self::LocalZipToS3(report) => report.has_errors(),
            Self::Local(report) => report.has_errors(),
            Self::DryRun(report) => report.has_errors(),
        }
    }

    pub(super) fn summary(&self) -> Option<&unspool::SyncSummary> {
        match self {
            Self::S3(report) => Some(&report.summary),
            Self::LocalZipToS3(report) => Some(&report.summary),
            Self::Local(report) => Some(&report.summary),
            Self::DryRun(_) => None,
        }
    }

    pub(super) fn dry_run_summary(&self) -> Option<&unspool::UnzipDryRunSummary> {
        match self {
            Self::DryRun(report) => Some(&report.summary),
            Self::S3(_) | Self::LocalZipToS3(_) | Self::Local(_) => None,
        }
    }

    pub(super) fn source(&self) -> String {
        match self {
            Self::S3(report) => report.source.uri(),
            Self::LocalZipToS3(report) => report.source_zip.clone(),
            Self::Local(report) => report.source_zip.clone(),
            Self::DryRun(report) => report.source_zip.clone(),
        }
    }

    pub(super) fn destination(&self) -> String {
        match self {
            Self::S3(report) => report.destination.uri(),
            Self::LocalZipToS3(report) => report.destination.uri(),
            Self::Local(report) => report.destination_dir.clone(),
            Self::DryRun(report) => report.destination.clone(),
        }
    }

    pub(super) fn operations(&self) -> &[unspool::ObjectReport] {
        match self {
            Self::S3(report) => &report.operations,
            Self::LocalZipToS3(report) => &report.operations,
            Self::Local(report) => &report.operations,
            Self::DryRun(_) => &[],
        }
    }

    pub(super) fn dry_run_operations(&self) -> &[unspool::DryRunObjectReport] {
        match self {
            Self::DryRun(report) => &report.operations,
            Self::S3(_) | Self::LocalZipToS3(_) | Self::Local(_) => &[],
        }
    }

    pub(super) fn diagnostics_line(&self) -> Option<String> {
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
