#!/usr/bin/env python3
"""Unit tests for mutate-fixture helpers."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("mutate-fixture.py")
SPEC = importlib.util.spec_from_file_location("mutate_fixture", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
mutate_fixture = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mutate_fixture)


class SafeManifestPathTests(unittest.TestCase):
    def test_accepts_relative_posix_paths(self) -> None:
        self.assertEqual(
            mutate_fixture.safe_manifest_path({"path": "dir/subdir/file.bin"}),
            Path("dir", "subdir", "file.bin"),
        )

    def test_rejects_parent_segments(self) -> None:
        with self.assertRaises(mutate_fixture.FixtureMutationError):
            mutate_fixture.safe_manifest_path({"path": "dir/../file.bin"})

    def test_rejects_absolute_posix_paths(self) -> None:
        with self.assertRaises(mutate_fixture.FixtureMutationError):
            mutate_fixture.safe_manifest_path({"path": "/tmp/file.bin"})

    def test_rejects_windows_drive_paths(self) -> None:
        with self.assertRaises(mutate_fixture.FixtureMutationError):
            mutate_fixture.safe_manifest_path({"path": "C:temp/file.bin"})

    def test_rejects_backslash_paths(self) -> None:
        with self.assertRaises(mutate_fixture.FixtureMutationError):
            mutate_fixture.safe_manifest_path({"path": "dir\\file.bin"})


if __name__ == "__main__":
    unittest.main()
