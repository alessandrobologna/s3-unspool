# Repository Layout

| Path | Purpose |
| --- | --- |
| `crates/s3-unspool` | Published Rust library crate. |
| `crates/s3-unspool-cli` | Published CLI crate; installs `s3-unspool`. |
| `tools/lambda-benchmark` | SAM/Cargo Lambda benchmark harness and runner. |
| `tools/fixturegen` | Local fixture generation package. |
| `docs/` | Tutorials, how-to guides, reference, explanation, and generated chart assets. |

The Lambda package is repository tooling. The published packages are
`s3-unspool` for the library and `s3-unspool-cli` for the command-line binary.
