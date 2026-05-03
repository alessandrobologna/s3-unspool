#!/usr/bin/env python3
"""Unit tests for mutate-fixture helpers."""

from __future__ import annotations

import unittest
from pathlib import Path

from s3_unspool_fixtures import mutate


class SafeManifestPathTests(unittest.TestCase):
    def test_accepts_relative_posix_paths(self) -> None:
        self.assertEqual(
            mutate.safe_manifest_path({"path": "dir/subdir/file.bin"}),
            Path("dir", "subdir", "file.bin"),
        )

    def test_rejects_parent_segments(self) -> None:
        with self.assertRaises(mutate.FixtureMutationError):
            mutate.safe_manifest_path({"path": "dir/../file.bin"})

    def test_rejects_absolute_posix_paths(self) -> None:
        with self.assertRaises(mutate.FixtureMutationError):
            mutate.safe_manifest_path({"path": "/tmp/file.bin"})

    def test_rejects_windows_drive_paths(self) -> None:
        with self.assertRaises(mutate.FixtureMutationError):
            mutate.safe_manifest_path({"path": "C:temp/file.bin"})

    def test_rejects_backslash_paths(self) -> None:
        with self.assertRaises(mutate.FixtureMutationError):
            mutate.safe_manifest_path({"path": "dir\\file.bin"})

    def test_entry_profile_uses_manifest_profile(self) -> None:
        self.assertEqual(
            mutate.entry_profile(
                {"path": "docs/guide.md", "profile": "markdown"},
                "compressible",
                Path("docs/guide.md"),
            ),
            "markdown",
        )

    def test_entry_profile_infers_older_manifest_extension(self) -> None:
        self.assertEqual(
            mutate.entry_profile(
                {"path": "crates/core/src/module.rs"},
                "compressible",
                Path("crates/core/src/module.rs"),
            ),
            "rust",
        )


if __name__ == "__main__":
    unittest.main()
