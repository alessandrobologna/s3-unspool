#!/usr/bin/env python3
"""Generate deterministic repository-shaped benchmark fixture directories."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import shutil
import sys
from pathlib import Path

from s3_unspool_fixtures.content import (
    FixtureContentStream,
    allocate_profiles,
    generated_fixture_path,
)


SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)([a-zA-Z]*)$")
DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_COMPRESSIBLE_RATIO = 0.4
DEFAULT_INCOMPRESSIBLE_RATIO = 0.4


def main() -> int:
    args = parse_args()
    try:
        total_size = parse_size(args.total_size)
        chunk_size = parse_size(args.chunk_size)
        generate_fixture(
            output_dir=args.output_dir,
            file_count=args.files,
            total_size=total_size,
            seed=args.seed,
            clean=args.clean,
            manifest_path=args.manifest,
            compressible_ratio=args.compressible_ratio,
            incompressible_ratio=args.incompressible_ratio,
            max_depth=args.max_depth,
            chunk_size=chunk_size,
        )
    except FixtureError as err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a deterministic directory tree containing a random-looking "
            "mix of structured text/code, binary assets, and mixed files."
        )
    )
    parser.add_argument("output_dir", type=Path, help="directory to create")
    parser.add_argument(
        "--files",
        type=int,
        required=True,
        help="number of regular files to generate",
    )
    parser.add_argument(
        "--total-size",
        required=True,
        help="total payload size, for example 512MiB, 1GB, or 5000000",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1,
        help="deterministic seed used for paths, sizes, and content",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="delete the output directory first if it already exists",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        help="manifest path; defaults to <output_dir>.manifest.json",
    )
    parser.add_argument(
        "--compressible-ratio",
        type=float,
        default=DEFAULT_COMPRESSIBLE_RATIO,
        help="fraction of files filled with structured Markdown, code, config, and logs",
    )
    parser.add_argument(
        "--incompressible-ratio",
        type=float,
        default=DEFAULT_INCOMPRESSIBLE_RATIO,
        help="fraction of files filled with deterministic pseudo-random bytes",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=4,
        help="maximum generated directory depth",
    )
    parser.add_argument(
        "--chunk-size",
        default=str(DEFAULT_CHUNK_SIZE),
        help="write chunk size, for example 1MiB",
    )
    return parser.parse_args()


def generate_fixture(
    *,
    output_dir: Path,
    file_count: int,
    total_size: int,
    seed: int,
    clean: bool,
    manifest_path: Path | None,
    compressible_ratio: float,
    incompressible_ratio: float,
    max_depth: int,
    chunk_size: int,
) -> None:
    if file_count <= 0:
        raise FixtureError("--files must be greater than zero")
    if total_size < file_count:
        raise FixtureError("--total-size must be at least --files so each file gets one byte")
    if chunk_size <= 0:
        raise FixtureError("--chunk-size must be greater than zero")
    if max_depth <= 0:
        raise FixtureError("--max-depth must be greater than zero")
    if compressible_ratio < 0 or incompressible_ratio < 0:
        raise FixtureError("ratios must be non-negative")
    if compressible_ratio + incompressible_ratio > 1.0:
        raise FixtureError("--compressible-ratio + --incompressible-ratio cannot exceed 1")

    output_dir = output_dir.resolve()
    manifest_path = (
        manifest_path.resolve()
        if manifest_path
        else output_dir.with_name(f"{output_dir.name}.manifest.json")
    )
    prepare_output_dir(output_dir, clean)

    rng = random.Random(seed)
    sizes = allocate_sizes(file_count, total_size, rng)
    kinds = allocate_kinds(file_count, compressible_ratio, incompressible_ratio, rng)
    profiles = allocate_profiles(kinds, seed)
    entries = []
    totals_by_kind: dict[str, dict[str, int]] = {}
    totals_by_profile: dict[str, dict[str, int]] = {}

    for index, (size, kind, profile) in enumerate(zip(sizes, kinds, profiles, strict=True)):
        relative_path = generated_fixture_path(index, kind, size, max_depth, rng, profile)
        path = output_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        digest = write_file(path, index, kind, profile, size, seed, chunk_size)
        entries.append(
            {
                "path": relative_path.as_posix(),
                "kind": kind,
                "profile": profile,
                "size": size,
                "sha256": digest,
            }
        )
        stats = totals_by_kind.setdefault(kind, {"files": 0, "bytes": 0})
        stats["files"] += 1
        stats["bytes"] += size
        profile_stats = totals_by_profile.setdefault(profile, {"files": 0, "bytes": 0})
        profile_stats["files"] += 1
        profile_stats["bytes"] += size

    manifest = {
        "version": 1,
        "seed": seed,
        "files": file_count,
        "total_size": total_size,
        "compressible_ratio": compressible_ratio,
        "incompressible_ratio": incompressible_ratio,
        "mixed_ratio": 1.0 - compressible_ratio - incompressible_ratio,
        "classes": totals_by_kind,
        "profiles": totals_by_profile,
        "entries": entries,
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")

    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "manifest": str(manifest_path),
                "files": file_count,
                "total_size": total_size,
                "classes": totals_by_kind,
                "profiles": totals_by_profile,
            },
            indent=2,
            sort_keys=True,
        )
    )


def prepare_output_dir(output_dir: Path, clean: bool) -> None:
    if output_dir.exists() and clean:
        if not output_dir.is_dir():
            raise FixtureError(f"{output_dir} exists and is not a directory")
        shutil.rmtree(output_dir)

    if output_dir.exists():
        if not output_dir.is_dir():
            raise FixtureError(f"{output_dir} exists and is not a directory")
        if any(output_dir.iterdir()):
            raise FixtureError(f"{output_dir} is not empty; use --clean to replace it")
    else:
        output_dir.mkdir(parents=True)


def parse_size(value: str) -> int:
    match = SIZE_RE.match(value.strip())
    if not match:
        raise FixtureError(f"invalid size: {value}")
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
        raise FixtureError(f"unsupported size suffix: {suffix}")
    size = int(amount * multiplier)
    if size < 0:
        raise FixtureError("size must be non-negative")
    return size


def allocate_sizes(file_count: int, total_size: int, rng: random.Random) -> list[int]:
    remaining = total_size - file_count
    weights = [min(rng.paretovariate(1.35), 100.0) for _ in range(file_count)]
    weight_sum = sum(weights)
    raw = [(remaining * weight / weight_sum) for weight in weights]
    allocations = [int(value) for value in raw]
    remainder = remaining - sum(allocations)
    fractions = sorted(
        range(file_count),
        key=lambda index: (raw[index] - allocations[index], weights[index]),
        reverse=True,
    )
    for index in fractions[:remainder]:
        allocations[index] += 1
    return [allocation + 1 for allocation in allocations]


def allocate_kinds(
    file_count: int,
    compressible_ratio: float,
    incompressible_ratio: float,
    rng: random.Random,
) -> list[str]:
    ratios = [
        ("compressible", compressible_ratio),
        ("incompressible", incompressible_ratio),
        ("mixed", 1.0 - compressible_ratio - incompressible_ratio),
    ]
    raw = [(name, file_count * ratio) for name, ratio in ratios]
    counts = {name: int(value) for name, value in raw}
    remainder = file_count - sum(counts.values())
    fractions = sorted(
        raw,
        key=lambda item: item[1] - int(item[1]),
        reverse=True,
    )
    for name, _value in fractions[:remainder]:
        counts[name] += 1

    kinds = [
        kind
        for kind in ("compressible", "incompressible", "mixed")
        for _ in range(counts[kind])
    ]
    rng.shuffle(kinds)
    return kinds


def write_file(
    path: Path,
    index: int,
    kind: str,
    profile: str,
    size: int,
    seed: int,
    chunk_size: int,
) -> str:
    digest = hashlib.sha256()
    stream = FixtureContentStream(seed=seed, index=index, kind=kind, profile=profile)
    remaining = size
    with path.open("wb") as file:
        while remaining:
            length = min(chunk_size, remaining)
            data = stream.next_bytes(length)
            file.write(data)
            digest.update(data)
            remaining -= length
    return digest.hexdigest()


class FixtureError(Exception):
    pass


if __name__ == "__main__":
    raise SystemExit(main())
