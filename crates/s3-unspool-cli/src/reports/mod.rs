mod destination;
mod unzip;
mod zip;

use std::path::PathBuf;

use serde::Serialize;

pub(crate) use destination::ReportDestination;
pub(crate) use unzip::{UnzipCommandReport, unzip_transcript};
pub(crate) use zip::{ZipCommandReport, zip_transcript};

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
