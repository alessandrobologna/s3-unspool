#!/usr/bin/env python3
"""Unit tests for generate-fixture helpers."""

from __future__ import annotations

import hashlib
import json
from contextlib import redirect_stdout
import io
from pathlib import Path, PurePosixPath
import tempfile
import unittest
import zipfile

from s3_unspool_fixtures import generate


class GenerateFixtureTests(unittest.TestCase):
    def test_compressible_fixture_uses_repo_like_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output_dir = root / "fixture"
            manifest_path = root / "fixture.manifest.json"

            run_generate(
                output_dir=output_dir,
                file_count=48,
                total_size=1024 * 1024,
                seed=42,
                clean=False,
                manifest_path=manifest_path,
                compressible_ratio=1.0,
                incompressible_ratio=0.0,
                max_depth=4,
                chunk_size=64 * 1024,
            )

            manifest = json.loads(manifest_path.read_text())
            profiles = set(manifest["profiles"])
            extensions = {PurePosixPath(entry["path"]).suffix for entry in manifest["entries"]}

            self.assertIn("markdown", profiles)
            self.assertIn("typescript", profiles)
            self.assertIn("json", profiles)
            self.assertIn("rust", profiles)
            self.assertIn(".md", extensions)
            self.assertIn(".ts", extensions)
            self.assertIn(".json", extensions)
            self.assertIn(".rs", extensions)
            ratio = zip_ratio(output_dir)
            self.assertGreater(ratio, 0.05)
            self.assertLess(ratio, 0.7)

    def test_manifest_entries_match_generated_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            output_dir = root / "fixture"
            manifest_path = root / "fixture.manifest.json"

            run_generate(
                output_dir=output_dir,
                file_count=15,
                total_size=128 * 1024,
                seed=7,
                clean=False,
                manifest_path=manifest_path,
                compressible_ratio=0.4,
                incompressible_ratio=0.4,
                max_depth=4,
                chunk_size=16 * 1024,
            )

            manifest = json.loads(manifest_path.read_text())
            actual_size = 0
            for entry in manifest["entries"]:
                path = output_dir / entry["path"]
                data = path.read_bytes()
                actual_size += len(data)
                self.assertEqual(len(data), entry["size"])
                self.assertEqual(hashlib.sha256(data).hexdigest(), entry["sha256"])

            self.assertEqual(actual_size, manifest["total_size"])

    def test_generation_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first_manifest_path = root / "first.manifest.json"
            second_manifest_path = root / "second.manifest.json"
            kwargs = {
                "file_count": 20,
                "total_size": 256 * 1024,
                "seed": 99,
                "clean": False,
                "compressible_ratio": 0.4,
                "incompressible_ratio": 0.4,
                "max_depth": 4,
                "chunk_size": 32 * 1024,
            }

            run_generate(
                output_dir=root / "first",
                manifest_path=first_manifest_path,
                **kwargs,
            )
            run_generate(
                output_dir=root / "second",
                manifest_path=second_manifest_path,
                **kwargs,
            )

            self.assertEqual(
                json.loads(first_manifest_path.read_text()),
                json.loads(second_manifest_path.read_text()),
            )


def run_generate(**kwargs: object) -> None:
    with redirect_stdout(io.StringIO()):
        generate.generate_fixture(**kwargs)


def zip_ratio(root: Path) -> float:
    zip_path = root.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                archive.write(path, path.relative_to(root).as_posix())
    raw_size = sum(path.stat().st_size for path in root.rglob("*") if path.is_file())
    return zip_path.stat().st_size / raw_size


if __name__ == "__main__":
    unittest.main()
