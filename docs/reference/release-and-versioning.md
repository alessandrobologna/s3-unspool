# Release and Versioning

Pre-release versions, when available, use standard Cargo SemVer pre-release
identifiers such as `0.1.0-alpha.1`, `0.1.0-beta.1`, or `0.1.0-rc.1`.
Consumers opt into a pre-release explicitly:

```sh
cargo add s3-unspool@0.1.0-beta.6
```

Releases are published by the manual `Publish s3-unspool` GitHub Actions
workflow. The workflow keeps `s3-unspool` and `s3-unspool-cli` in lockstep,
builds `cargo-dist` CLI archives for GitHub Releases, publishes the library
crate first, waits for registry propagation, publishes the CLI crate, and only
then creates the matching `v<version>` GitHub Release.

Configure both crates on crates.io to trust this repository's
`publish-s3-unspool.yml` workflow and the `release` GitHub environment before
running it.

Trusted Publishing can publish new versions of an existing crate, but it cannot
create a crate name for the first time. Bootstrap each published crate once with
a manual `cargo publish` using an owner API token, then enable the trusted
publisher before running the workflow.

If a run publishes one crate and fails on the next one, do not rerun the same
version through the workflow. Publish the missing crate manually or bump every
package version before trying again.
