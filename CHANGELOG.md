# Changelog

All notable changes to this project are documented here.

## Unreleased

### Changed

- Release notes now include the exact `cargo binstall` command and a first-parent
  commit list for every GitHub Release.

## 0.1.0-beta.5 - 2026-05-03

### Fixed

- Fixed `cargo-binstall` metadata so Unix and macOS release archives resolve the
  `s3-unspool` binary inside the `cargo-dist` top-level archive directory.
- Kept the Windows `cargo-binstall` metadata pointed at the zip archive root.

### Documentation

- Documented that prerelease CLI installs must include an explicit version.

## 0.1.0-beta.4 - 2026-05-03

### Fixed

- Added crates.io publish-target preflights so missing crate bootstrap or
  already-published versions fail before release artifacts are built or uploaded.
- Documented the manual Trusted Publishing bootstrap and partial-publish recovery
  path.
