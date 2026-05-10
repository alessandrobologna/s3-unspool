# Changelog

All notable changes to this project are documented here.

## Unreleased

## 0.1.0 - 2026-05-10

### Added

- Prepared the first stable release of the `s3-unspool` library and
  `s3-unspool-cli` command-line tool.
- Included the current archive feature set: bounded-memory S3 ZIP extraction,
  local ZIP helpers, cataloged incremental extraction, selective glob-based
  extraction, safe conditional destination writes, Deflate support, and
  Zstandard ZIP method 93 support.
- Included the Diataxis-style documentation set, benchmark snapshots, fixture
  tooling docs, release/versioning guidance, and storage economics guidance.

### Changed

- Promoted the library and CLI crates from `0.1.0-beta.6` to the first stable
  `0.1.0` release.
- Kept the builder-style Rust option APIs and explicit policy enums introduced
  during the beta series as the stable `0.1.0` API surface.
- Updated install and versioning documentation to use stable Cargo and
  `cargo-binstall` commands by default.

### Fixed

- Scoped a Unix-only path-case helper so Windows builds stay warning-clean.

## 0.1.0-beta.6 - 2026-05-04

### Added

- Added a Diataxis-style documentation tree under `docs/`, with tutorials,
  how-to guides, reference pages, and explanation pages.
- Added storage economics guidance and generated SVG chart assets for
  compressed-storage scenarios.
- Added read-only accessors for `SyncOptions` so callers can inspect
  extraction configuration without mutating public fields.

### Changed

- Replaced mutable public option fields with builder-style Rust APIs for unzip,
  zip, retry, progress, and scheduler configuration.
- Replaced sharp boolean options with explicit policies:
  `DestinationCleanup`, `ComparisonMode`, and `ConflictPolicy`.
- Replaced positional adaptive source-window sizing with
  `AdaptiveSourceWindow`.
- Updated the CLI, Lambda benchmark harness, examples, and tests to use the new
  builder-style Rust interface.
- Release notes now include the exact `cargo binstall` command and a first-parent
  commit list for every GitHub Release.

### Fixed

- Restored Lambda payload-to-options assertions after the options refactor.
- Updated validation messages and tracing fields to use the new public option
  API names.

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
