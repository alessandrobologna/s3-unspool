# Install the CLI

Use the CLI for smoke tests, local workflows, and S3 zip/unzip operations from a
terminal.

## Install a Prebuilt Release

Install the pre-release binary with `cargo-binstall` when GitHub Release
artifacts are available:

```sh
cargo binstall s3-unspool-cli --version 0.1.0-beta.5
```

The package name is `s3-unspool-cli`, but the installed command is
`s3-unspool`. Current releases are prereleases, so include the explicit version.
An unqualified `cargo binstall s3-unspool-cli` resolves stable versions only.

## Build from a Checkout

```sh
cargo build --release -p s3-unspool-cli --bin s3-unspool
```

The built binary is:

```sh
./target/release/s3-unspool
```

During development, run commands through Cargo:

```sh
cargo run -p s3-unspool-cli -- \
  unzip s3://my-bucket/releases/site.zip s3://my-bucket/www/
```

## See Also

- [CLI Reference](../reference/cli.md)
- [First Extract](../tutorials/first-extract.md)
