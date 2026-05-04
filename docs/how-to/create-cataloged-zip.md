# Create a Cataloged ZIP

Cataloged ZIPs include `.s3-unspool/catalog.v1.json`, which stores each file
path and MD5 digest. Extraction can use that catalog to skip unchanged entries
before decompression.

## Zip a Local Directory to S3

```sh
s3-unspool zip ./site s3://my-bucket/releases/site.zip
```

To choose compression:

```sh
s3-unspool zip \
  --compression zstd \
  ./site \
  s3://my-bucket/releases/site.zip
```

Zstd writes ZIP method 93 and may not open in OS-native ZIP tools. Use Deflate
when broad compatibility matters.

## Zip an S3 Prefix to S3

```sh
s3-unspool zip \
  s3://my-bucket/www/ \
  s3://my-bucket/releases/site.zip
```

The destination ZIP object cannot be inside the listed source prefix. That
prevents an existing archive from being accidentally included in the new
archive.

## Create a Plain ZIP

Use `--no-catalog` only when you need a plain ZIP without the embedded catalog:

```sh
s3-unspool zip --no-catalog ./site ./site.zip
```

## Preview Without Writing

```sh
s3-unspool zip --dry-run --report ./site s3://my-bucket/releases/site.zip
```

## See Also

- [CLI Reference](../reference/cli.md)
- [Incremental Extraction](../explanation/incremental-extraction.md)
