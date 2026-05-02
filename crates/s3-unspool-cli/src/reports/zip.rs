use std::time::Duration;

use s3_unspool as unspool;

use super::{ReportDestination, write_report};
use crate::ui::{Transcript, format_bytes, format_elapsed, format_upload_speed, plural};

pub(crate) enum ZipCommandReport {
    Upload(unspool::UploadReport),
    S3Prefix(unspool::S3PrefixUploadReport),
    Local(unspool::LocalZipReport),
    DryRun(unspool::ZipDryRunReport),
}

impl ZipCommandReport {
    pub(crate) async fn write(
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
            Self::Upload(report) => Some(report.include_catalog),
            Self::S3Prefix(report) => Some(report.include_catalog),
            Self::Local(report) => Some(report.include_catalog),
            Self::DryRun(report) => Some(report.include_catalog),
        }
    }
}

pub(crate) fn zip_transcript(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::Theme;

    #[test]
    fn renders_zip_transcript() {
        let report = unspool::UploadReport {
            source_dir: "./site".to_string(),
            destination: unspool::S3Object::parse("s3://bucket/site.zip").unwrap(),
            files: 2,
            directories: 0,
            include_catalog: true,
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
        let report = unspool::UploadReport {
            source_dir: "./site".to_string(),
            destination: unspool::S3Object::parse("s3://bucket/site.zip").unwrap(),
            files: 2,
            directories: 1,
            include_catalog: true,
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
            "✓ Zip complete\n  └ 2 files, 1 directory, 4.0 MiB uncompressed, 3.0 MiB ZIP\n    s3://bucket/site.zip\n    Report:\n      Source: ./site\n      Destination: s3://bucket/site.zip\n      Files: 2 files\n      Directories: 1 directory\n      Uncompressed: 4.0 MiB\n      Catalog: included\n      ZIP: 3.0 MiB\n      Wall time: 00:02\n      Zip speed: 1.50 MiB/s"
        );
    }

    #[test]
    fn renders_zip_dry_run_transcript() {
        let report = unspool::ZipDryRunReport {
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
}
