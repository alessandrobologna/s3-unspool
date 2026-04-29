#!/usr/bin/env python3
"""Create an update fixture by changing a deterministic subset of files."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import shutil
import sys
from pathlib import Path, PurePosixPath, PureWindowsPath


SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)([a-zA-Z]*)$")
DEFAULT_CHUNK_SIZE = 1024 * 1024


def main() -> int:
    args = parse_args()
    try:
        chunk_size = parse_size(args.chunk_size)
        mutate_fixture(
            source_dir=args.source_dir,
            output_dir=args.output_dir,
            manifest_path=args.manifest,
            output_manifest_path=args.output_manifest,
            change_ratio=args.change_ratio,
            changed_files=args.changed_files,
            seed=args.seed,
            clean=args.clean,
            copy_mode=args.copy_mode,
            chunk_size=chunk_size,
        )
    except FixtureMutationError as err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Copy an existing generated fixture and rewrite a deterministic "
            "subset of files while preserving paths, sizes, and file classes."
        )
    )
    parser.add_argument("source_dir", type=Path, help="existing fixture directory")
    parser.add_argument("output_dir", type=Path, help="directory to create")
    parser.add_argument(
        "--manifest",
        type=Path,
        help="source manifest; defaults to <source_dir>.manifest.json",
    )
    parser.add_argument(
        "--output-manifest",
        type=Path,
        help="output manifest; defaults to <output_dir>.manifest.json",
    )
    parser.add_argument(
        "--change-ratio",
        type=float,
        default=0.10,
        help="fraction of files to rewrite; default: 0.10",
    )
    parser.add_argument(
        "--changed-files",
        type=int,
        help="exact number of files to rewrite; overrides --change-ratio",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=2,
        help="deterministic seed used to select and rewrite files",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="delete output_dir first if it already exists",
    )
    parser.add_argument(
        "--copy-mode",
        choices=("hardlink", "copy"),
        default="hardlink",
        help="hardlink unchanged files when possible, or fully copy them",
    )
    parser.add_argument(
        "--chunk-size",
        default=str(DEFAULT_CHUNK_SIZE),
        help="write chunk size for changed files, for example 1MiB",
    )
    return parser.parse_args()


def mutate_fixture(
    *,
    source_dir: Path,
    output_dir: Path,
    manifest_path: Path | None,
    output_manifest_path: Path | None,
    change_ratio: float,
    changed_files: int | None,
    seed: int,
    clean: bool,
    copy_mode: str,
    chunk_size: int,
) -> None:
    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()
    manifest_path = (
        manifest_path.resolve()
        if manifest_path
        else source_dir.with_name(f"{source_dir.name}.manifest.json")
    )
    output_manifest_path = (
        output_manifest_path.resolve()
        if output_manifest_path
        else output_dir.with_name(f"{output_dir.name}.manifest.json")
    )

    if not source_dir.is_dir():
        raise FixtureMutationError(f"{source_dir} is not a directory")
    if source_dir == output_dir:
        raise FixtureMutationError("source_dir and output_dir must be different")
    if not manifest_path.is_file():
        raise FixtureMutationError(f"manifest not found: {manifest_path}")
    if chunk_size <= 0:
        raise FixtureMutationError("--chunk-size must be greater than zero")
    if changed_files is not None and changed_files < 0:
        raise FixtureMutationError("--changed-files cannot be negative")
    if not 0.0 <= change_ratio <= 1.0:
        raise FixtureMutationError("--change-ratio must be between 0 and 1")

    manifest = json.loads(manifest_path.read_text())
    entries = manifest.get("entries")
    if not isinstance(entries, list) or not entries:
        raise FixtureMutationError("manifest must contain a non-empty entries list")

    count = mutation_count(len(entries), change_ratio, changed_files)
    selected = set(random.Random(seed).sample(range(len(entries)), count))

    prepare_output_dir(output_dir, clean)
    copy_fixture_tree(source_dir, output_dir, copy_mode)

    mutated_entries = []
    changed_bytes = 0
    classes: dict[str, dict[str, int]] = {}

    for index, entry in enumerate(entries):
        relative_path = safe_manifest_path(entry)
        kind = entry_string(entry, "kind")
        size = entry_int(entry, "size")
        original_sha256 = entry_string(entry, "sha256")
        path = output_dir / relative_path
        changed = index in selected
        new_sha256 = original_sha256

        if changed:
            if path.exists():
                path.unlink()
            new_sha256 = write_changed_file(
                path=path,
                index=index,
                kind=kind,
                size=size,
                seed=seed,
                chunk_size=chunk_size,
            )
            if new_sha256 == original_sha256:
                raise FixtureMutationError(f"mutation did not change content: {relative_path}")
            changed_bytes += size

        stats = classes.setdefault(kind, {"files": 0, "bytes": 0})
        stats["files"] += 1
        stats["bytes"] += size
        mutated_entries.append(
            {
                **entry,
                "sha256": new_sha256,
                "changed": changed,
                "original_sha256": original_sha256,
            }
        )

    output_manifest = {
        **manifest,
        "version": 2,
        "entries": mutated_entries,
        "classes": classes,
        "mutation": {
            "source_dir": str(source_dir),
            "source_manifest": str(manifest_path),
            "output_dir": str(output_dir),
            "seed": seed,
            "change_ratio": change_ratio,
            "changed_files": count,
            "unchanged_files": len(entries) - count,
            "changed_bytes": changed_bytes,
            "copy_mode": copy_mode,
        },
    }
    output_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    output_manifest_path.write_text(json.dumps(output_manifest, indent=2, sort_keys=True) + "\n")

    print(
        json.dumps(
            {
                "source_dir": str(source_dir),
                "output_dir": str(output_dir),
                "manifest": str(output_manifest_path),
                "files": len(entries),
                "changed_files": count,
                "unchanged_files": len(entries) - count,
                "changed_bytes": changed_bytes,
                "copy_mode": copy_mode,
            },
            indent=2,
            sort_keys=True,
        )
    )


def mutation_count(file_count: int, change_ratio: float, changed_files: int | None) -> int:
    if changed_files is not None:
        return min(changed_files, file_count)
    return min(math.ceil(file_count * change_ratio), file_count)


def prepare_output_dir(output_dir: Path, clean: bool) -> None:
    if output_dir.exists() and clean:
        if not output_dir.is_dir():
            raise FixtureMutationError(f"{output_dir} exists and is not a directory")
        shutil.rmtree(output_dir)
    if output_dir.exists():
        raise FixtureMutationError(f"{output_dir} already exists; use --clean to replace it")


def copy_fixture_tree(source_dir: Path, output_dir: Path, copy_mode: str) -> None:
    copy_function = shutil.copy2 if copy_mode == "copy" else hardlink_or_copy
    shutil.copytree(source_dir, output_dir, copy_function=copy_function)


def hardlink_or_copy(source: str, destination: str) -> None:
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)


def safe_manifest_path(entry: object) -> Path:
    raw_path = entry_string(entry, "path")
    posix_path = PurePosixPath(raw_path)
    windows_path = PureWindowsPath(raw_path)
    if (
        "\\" in raw_path
        or posix_path.is_absolute()
        or windows_path.drive
        or windows_path.anchor
        or not posix_path.parts
        or ".." in posix_path.parts
    ):
        raise FixtureMutationError(f"unsafe manifest path: {raw_path}")
    return Path(*posix_path.parts)


def entry_string(entry: object, key: str) -> str:
    if not isinstance(entry, dict):
        raise FixtureMutationError("manifest entry is not an object")
    value = entry.get(key)
    if not isinstance(value, str) or not value:
        raise FixtureMutationError(f"manifest entry has invalid {key}")
    return value


def entry_int(entry: object, key: str) -> int:
    if not isinstance(entry, dict):
        raise FixtureMutationError("manifest entry is not an object")
    value = entry.get(key)
    if not isinstance(value, int) or value < 0:
        raise FixtureMutationError(f"manifest entry has invalid {key}")
    return value


def write_changed_file(
    *,
    path: Path,
    index: int,
    kind: str,
    size: int,
    seed: int,
    chunk_size: int,
) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    stream = MutatedContentStream(seed=seed, index=index, kind=kind)
    remaining = size
    with path.open("wb") as file:
        while remaining:
            length = min(chunk_size, remaining)
            data = stream.next_bytes(length)
            file.write(data)
            digest.update(data)
            remaining -= length
    return digest.hexdigest()


class MutatedContentStream:
    def __init__(self, *, seed: int, index: int, kind: str) -> None:
        if kind not in {"compressible", "incompressible", "mixed"}:
            raise FixtureMutationError(f"unsupported file kind: {kind}")
        self.kind = kind
        self.rng = random.Random((seed << 48) ^ (index << 8) ^ 0xA5A5)
        self.offset = 0
        self.pattern = (
            f"file={index:08d} mutation_seed={seed:08d} "
            "status=updated route=/assets/app.js method=GET "
            "message='changed deployment fixture content'\n"
        ).encode()

    def next_bytes(self, length: int) -> bytes:
        if self.kind == "incompressible":
            return self.rng.randbytes(length)
        if self.kind == "compressible":
            return self._compressible(length)
        return self._mixed(length)

    def _compressible(self, length: int) -> bytes:
        data = bytearray()
        while len(data) < length:
            data.extend(self.pattern)
        self.offset += length
        return bytes(data[:length])

    def _mixed(self, length: int) -> bytes:
        data = bytearray()
        block_index = self.offset // 4096
        while len(data) < length:
            block_remaining = 4096 - ((self.offset + len(data)) % 4096)
            want = min(block_remaining, length - len(data))
            if block_index % 4 == 3:
                data.extend(self.rng.randbytes(want))
            else:
                data.extend(self._pattern_slice(want))
            if (self.offset + len(data)) % 4096 == 0:
                block_index += 1
        self.offset += length
        return bytes(data)

    def _pattern_slice(self, length: int) -> bytes:
        data = bytearray()
        while len(data) < length:
            data.extend(self.pattern)
        return bytes(data[:length])


def parse_size(value: str) -> int:
    match = SIZE_RE.match(value.strip())
    if not match:
        raise FixtureMutationError(f"invalid size: {value}")
    amount = float(match.group(1))
    suffix = match.group(2).lower()
    units = {
        "": 1,
        "b": 1,
        "k": 1024,
        "kb": 1000,
        "kib": 1024,
        "m": 1024**2,
        "mb": 1000**2,
        "mib": 1024**2,
        "g": 1024**3,
        "gb": 1000**3,
        "gib": 1024**3,
        "t": 1024**4,
        "tb": 1000**4,
        "tib": 1024**4,
    }
    multiplier = units.get(suffix)
    if multiplier is None:
        raise FixtureMutationError(f"unsupported size suffix: {suffix}")
    size = int(amount * multiplier)
    if size <= 0:
        raise FixtureMutationError("size must be greater than zero")
    return size


class FixtureMutationError(Exception):
    pass


if __name__ == "__main__":
    raise SystemExit(main())
