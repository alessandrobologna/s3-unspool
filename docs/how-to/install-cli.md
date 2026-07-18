# Install the CLI

Use the CLI for smoke tests, local workflows, and S3 zip/unzip operations from a
terminal.

## Install a Prebuilt Release

Install the binary with `cargo-binstall` when GitHub Release artifacts are
available:

```sh
cargo binstall s3-unspool-cli
```

The package name is `s3-unspool-cli`, but the installed command is
`s3-unspool`. To pin the first stable release explicitly, use
`cargo binstall s3-unspool-cli@0.1.0`.

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
