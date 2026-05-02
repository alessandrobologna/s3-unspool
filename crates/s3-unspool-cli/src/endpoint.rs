use std::path::PathBuf;

use clap::ArgMatches;
use s3_unspool as unspool;

pub(crate) enum TreeSource {
    Local(PathBuf),
    S3(unspool::S3Prefix),
}

impl TreeSource {
    pub(crate) fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::S3(prefix) => prefix.uri(),
        }
    }
}

pub(crate) enum TreeDestination {
    Local(PathBuf),
    S3(unspool::S3Prefix),
}

impl TreeDestination {
    pub(crate) fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::S3(prefix) => prefix.uri(),
        }
    }
}

pub(crate) enum ZipSource {
    Local(PathBuf),
    S3(unspool::S3Object),
}

impl ZipSource {
    pub(crate) fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::S3(object) => object.uri(),
        }
    }
}

pub(crate) enum ZipDestination {
    Local(PathBuf),
    S3(unspool::S3Object),
}

impl ZipDestination {
    pub(crate) fn display(&self) -> String {
        match self {
            Self::Local(path) => path.display().to_string(),
            Self::S3(object) => object.uri(),
        }
    }
}

pub(crate) fn parse_tree_source(value: &str) -> Result<TreeSource, Box<dyn std::error::Error>> {
    reject_file_uri(value)?;
    if value.starts_with("s3://") {
        Ok(TreeSource::S3(unspool::S3Prefix::parse(value)?))
    } else {
        Ok(TreeSource::Local(PathBuf::from(value)))
    }
}

pub(crate) fn parse_tree_destination(
    value: &str,
) -> Result<TreeDestination, Box<dyn std::error::Error>> {
    reject_file_uri(value)?;
    if value.starts_with("s3://") {
        Ok(TreeDestination::S3(unspool::S3Prefix::parse(value)?))
    } else {
        Ok(TreeDestination::Local(PathBuf::from(value)))
    }
}

pub(crate) fn parse_zip_source(value: &str) -> Result<ZipSource, Box<dyn std::error::Error>> {
    reject_file_uri(value)?;
    if value.starts_with("s3://") {
        Ok(ZipSource::S3(unspool::S3Object::parse(value)?))
    } else {
        Ok(ZipSource::Local(PathBuf::from(value)))
    }
}

pub(crate) fn parse_zip_destination(
    value: &str,
) -> Result<ZipDestination, Box<dyn std::error::Error>> {
    reject_file_uri(value)?;
    if value.starts_with("s3://") {
        Ok(ZipDestination::S3(unspool::S3Object::parse(value)?))
    } else {
        Ok(ZipDestination::Local(PathBuf::from(value)))
    }
}

pub(crate) fn validate_delete_extra_destination(
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

pub(crate) fn unzip_selection_from_matches(matches: &ArgMatches) -> unspool::UnzipSelection {
    let mut selection = unspool::UnzipSelection::new();
    if let Some(includes) = matches.get_many::<String>("include") {
        for include in includes {
            selection = selection.include(include.clone());
        }
    }
    if let Some(excludes) = matches.get_many::<String>("exclude") {
        for exclude in excludes {
            selection = selection.exclude(exclude.clone());
        }
    }
    selection
}

pub(crate) fn validate_delete_extra_selection(
    delete_extra: bool,
    selection: &unspool::UnzipSelection,
) -> Result<(), Box<dyn std::error::Error>> {
    if delete_extra && !selection.is_empty() {
        Err("--delete-extra cannot be combined with --include or --exclude".into())
    } else {
        Ok(())
    }
}

pub(crate) fn validate_diagnostics_source(
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

#[cfg(test)]
mod tests {
    use super::*;
    use s3_unspool as unspool;

    #[test]
    fn rejects_delete_extra_with_unzip_selection() {
        let selection = unspool::UnzipSelection::new().include("index.md");

        let err = validate_delete_extra_selection(true, &selection).unwrap_err();

        assert!(err.to_string().contains("--delete-extra"));
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
    fn builds_unzip_selection_from_repeated_flags() {
        let matches = crate::cli::cli()
            .try_get_matches_from([
                "s3-unspool",
                "unzip",
                "--include",
                "docs/**/*.md",
                "--include",
                "assets/**",
                "--exclude",
                "docs/drafts/**",
                "s3://source-bucket/archive.zip",
                "s3://destination-bucket/prefix/",
            ])
            .unwrap();

        let Some(("unzip", unzip)) = matches.subcommand() else {
            panic!("expected unzip subcommand");
        };

        let selection = unzip_selection_from_matches(unzip);

        assert_eq!(
            selection.as_patterns(),
            &[
                "docs/**/*.md".to_string(),
                "assets/**".to_string(),
                "!docs/drafts/**".to_string(),
            ]
        );
    }
}
