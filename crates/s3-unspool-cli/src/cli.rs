use clap::{Arg, ArgAction, ArgMatches, Command, value_parser};
use s3_unspool as unspool;

pub(crate) fn cli() -> Command {
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
                    Arg::new("include")
                        .long("include")
                        .value_name("PATTERN")
                        .help("Extract ZIP entries matching this gitignore-style pattern; repeat to include multiple patterns")
                        .action(ArgAction::Append),
                )
                .arg(
                    Arg::new("exclude")
                        .long("exclude")
                        .value_name("PATTERN")
                        .help("Exclude ZIP entries matching this gitignore-style pattern; repeat to exclude multiple patterns")
                        .action(ArgAction::Append),
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
                .arg(zip_compression_arg()),
        )
}

fn zip_compression_arg() -> Arg {
    let arg = Arg::new("compression")
        .long("compression")
        .value_name("METHOD")
        .help("Compression method for regular file entries")
        .default_value("deflate");

    #[cfg(feature = "zstd")]
    {
        arg.value_parser(["deflate", "zstd"])
    }
    #[cfg(not(feature = "zstd"))]
    {
        arg.value_parser(["deflate"])
    }
}

pub(crate) fn parse_zip_compression(
    matches: &ArgMatches,
) -> Result<unspool::ZipCompression, Box<dyn std::error::Error>> {
    match matches
        .get_one::<String>("compression")
        .map(String::as_str)
        .unwrap_or("deflate")
    {
        "deflate" => Ok(unspool::ZipCompression::Deflate),
        "zstd" => {
            #[cfg(feature = "zstd")]
            {
                Ok(unspool::ZipCompression::Zstd)
            }
            #[cfg(not(feature = "zstd"))]
            {
                Err("zstd compression requires the s3-unspool-cli `zstd` feature".into())
            }
        }
        other => Err(format!("unsupported ZIP compression method {other:?}").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3_unspool as unspool;

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
                "--include",
                "docs/**/*.md",
                "--exclude",
                "docs/drafts/**",
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
            extract
                .get_many::<String>("include")
                .map(|values| values.map(String::as_str).collect::<Vec<_>>()),
            Some(vec!["docs/**/*.md"])
        );
        assert_eq!(
            extract
                .get_many::<String>("exclude")
                .map(|values| values.map(String::as_str).collect::<Vec<_>>()),
            Some(vec!["docs/drafts/**"])
        );
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

    #[cfg(feature = "zstd")]
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
        assert_eq!(
            parse_zip_compression(upload).unwrap(),
            unspool::ZipCompression::Zstd
        );
    }

    #[cfg(not(feature = "zstd"))]
    #[test]
    fn rejects_zstd_zip_compression_without_feature() {
        let error = cli()
            .try_get_matches_from([
                "s3-unspool",
                "zip",
                "--compression",
                "zstd",
                "/tmp/site",
                "s3://destination-bucket/site.zip",
            ])
            .unwrap_err();

        assert!(error.to_string().contains("possible values: deflate"));
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
}
