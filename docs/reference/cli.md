# CLI Reference

The CLI command is `s3-unspool`. It supports `zip` and `unzip` across local and
S3 endpoints.

## Endpoint Matrix

```sh
s3-unspool zip   ./site                  ./site.zip
s3-unspool zip   ./site                  s3://my-bucket/site.zip
s3-unspool zip   s3://my-bucket/www/     ./site.zip
s3-unspool zip   s3://my-bucket/www/     s3://my-bucket/site.zip
s3-unspool unzip ./site.zip              ./site
s3-unspool unzip ./site.zip              s3://my-bucket/www/
s3-unspool unzip s3://my-bucket/site.zip ./site
s3-unspool unzip s3://my-bucket/site.zip s3://my-bucket/www/
```

## Zip Options

| Option | Meaning |
| --- | --- |
| `--dry-run` | Inspect the source tree and report what would be archived without creating a local ZIP or uploading an S3 object. |
| `--no-catalog` | Create a plain ZIP without `.s3-unspool/catalog.v1.json`. |
| `--compression deflate\|zstd` | Choose the compression method for regular file entries. Zstd writes ZIP method 93 and may not open in OS-native ZIP tools. |
| `--report` | Add a formatted zip report to the CLI transcript. |
| `--report=PATH` | Write the JSON zip report to a file. |

## Unzip Options

| Option | Meaning |
| --- | --- |
| `--dry-run` | Inspect the ZIP and destination, then report what would be created, replaced, skipped, or deleted without writing or deleting anything. |
| `--delete-extra` | Delete destination objects under the prefix that are not in the ZIP. |
| `--include PATTERN` | Extract ZIP entries matching this gitignore-style pattern. Repeat to include multiple patterns. |
| `--exclude PATTERN` | Exclude ZIP entries matching this gitignore-style pattern. Repeat to exclude multiple patterns. |
| `--concurrency <N>` | Maximum number of ZIP entries processed at once. The CLI default is `64`. |
| `--report` | Add a formatted operation report to the CLI transcript. |
| `--report=PATH` | Write the JSON operation report to a file. |
| `--diagnostics` | For `s3://` ZIP sources, add source scheduler, ranged `GetObject`, block cache, and destination `PutObject` retry counters to the JSON report. |
| `--ignore-catalog` | Ignore `.s3-unspool/catalog.v1.json` and compare existing destination objects by extracting and hashing each ZIP entry. |

Selection cannot be combined with `--delete-extra`, because unselected
destination objects are outside the restore scope.

## Global Options

| Option | Meaning |
| --- | --- |
| `--quiet` | Suppress human-readable status output. |
| `--color auto\|always\|never` | Control semantic color output. |

## See Also

- [Reports](reports.md)
- [Create a Cataloged ZIP](../how-to/create-cataloged-zip.md)
- [Extract Selected Entries](../how-to/extract-selected-entries.md)
