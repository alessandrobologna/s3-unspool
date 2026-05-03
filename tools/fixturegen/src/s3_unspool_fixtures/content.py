"""Shared deterministic content profiles for benchmark fixtures."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import random


@dataclass(frozen=True)
class ContentProfile:
    name: str
    extension: str
    weight: int
    stem: str
    directories: tuple[tuple[str, ...], ...]


COMPRESSIBLE_PROFILES = (
    ContentProfile(
        "markdown",
        ".md",
        16,
        "guide",
        (("docs",), ("docs", "reference"), ("docs", "how-to")),
    ),
    ContentProfile(
        "typescript",
        ".ts",
        14,
        "component",
        (("packages", "web", "src"), ("app", "src", "routes")),
    ),
    ContentProfile(
        "json",
        ".json",
        12,
        "metadata",
        (("config",), ("packages", "web", "fixtures"), ("data", "schemas")),
    ),
    ContentProfile(
        "javascript",
        ".js",
        10,
        "bundle",
        (("site", "assets"), ("packages", "cli", "lib"), ("scripts",)),
    ),
    ContentProfile(
        "rust",
        ".rs",
        8,
        "module",
        (("crates", "core", "src"), ("crates", "lambda", "src")),
    ),
    ContentProfile(
        "css",
        ".css",
        7,
        "styles",
        (("site", "styles"), ("packages", "web", "src", "styles")),
    ),
    ContentProfile(
        "html",
        ".html",
        6,
        "page",
        (("site", "pages"), ("public",)),
    ),
    ContentProfile(
        "python",
        ".py",
        6,
        "task",
        (("tools",), ("scripts",), ("tests",)),
    ),
    ContentProfile(
        "yaml",
        ".yaml",
        5,
        "workflow",
        ((".github", "workflows"), ("config", "deploy")),
    ),
    ContentProfile(
        "log",
        ".log",
        5,
        "deploy",
        (("logs",), ("var", "reports")),
    ),
    ContentProfile(
        "toml",
        ".toml",
        3,
        "manifest",
        (("config",), ("crates", "core")),
    ),
)

INCOMPRESSIBLE_PROFILES = (
    ContentProfile("png", ".png", 26, "image", (("public", "assets", "images"),)),
    ContentProfile("jpeg", ".jpg", 18, "photo", (("public", "assets", "images"),)),
    ContentProfile("font", ".woff2", 16, "font", (("public", "assets", "fonts"),)),
    ContentProfile("brotli", ".br", 14, "precompressed", (("public", "assets", "compressed"),)),
    ContentProfile("zip", ".zip", 12, "archive", (("vendor", "archives"),)),
    ContentProfile("binary", ".bin", 14, "blob", (("data", "binary"),)),
)

MIXED_PROFILES = (
    ContentProfile(
        "source-map",
        ".map",
        35,
        "chunk",
        (("site", "assets"), ("packages", "web", "dist")),
    ),
    ContentProfile(
        "bundled-js",
        ".js",
        25,
        "app",
        (("site", "assets"), ("packages", "web", "dist")),
    ),
    ContentProfile(
        "wasm",
        ".wasm",
        18,
        "module",
        (("public", "assets", "wasm"),),
    ),
    ContentProfile(
        "sqlite",
        ".db",
        12,
        "cache",
        (("data", "cache"),),
    ),
    ContentProfile(
        "packed",
        ".dat",
        10,
        "asset",
        (("public", "assets", "packed"),),
    ),
)

PROFILE_GROUPS = {
    "compressible": COMPRESSIBLE_PROFILES,
    "incompressible": INCOMPRESSIBLE_PROFILES,
    "mixed": MIXED_PROFILES,
}

PROFILE_BY_NAME = {
    profile.name: profile
    for profiles in PROFILE_GROUPS.values()
    for profile in profiles
}

PROFILE_KIND_BY_NAME = {
    profile.name: kind
    for kind, profiles in PROFILE_GROUPS.items()
    for profile in profiles
}

KIND_SALTS = {
    "compressible": 0xC0DE,
    "incompressible": 0xB17A,
    "mixed": 0xA55E7,
}

WORDS = (
    "artifact",
    "catalog",
    "checksum",
    "deployment",
    "endpoint",
    "fixture",
    "manifest",
    "pipeline",
    "prefix",
    "restore",
    "snapshot",
    "stream",
)

ROUTES = (
    "/",
    "/assets/app.js",
    "/api/deployments",
    "/docs/reference",
    "/health",
    "/static/chunk.js",
    "/worker/process",
)

OWNERS = (
    "platform",
    "runtime",
    "docs",
    "frontend",
    "storage",
    "observability",
)


def allocate_profiles(kinds: list[str], seed: int) -> list[str]:
    profiles_by_index = [""] * len(kinds)
    for kind, profile_group in PROFILE_GROUPS.items():
        indices = [index for index, item in enumerate(kinds) if item == kind]
        names = allocate_profile_names(len(indices), profile_group)
        random.Random((seed << 16) ^ KIND_SALTS[kind]).shuffle(names)
        for index, profile_name in zip(indices, names, strict=True):
            profiles_by_index[index] = profile_name
    return profiles_by_index


def allocate_profile_names(file_count: int, profiles: tuple[ContentProfile, ...]) -> list[str]:
    if file_count == 0:
        return []
    weight_sum = sum(profile.weight for profile in profiles)
    raw = [(profile, file_count * profile.weight / weight_sum) for profile in profiles]
    counts = {profile.name: int(value) for profile, value in raw}
    remainder = file_count - sum(counts.values())
    fractions = sorted(
        raw,
        key=lambda item: (item[1] - int(item[1]), item[0].weight),
        reverse=True,
    )
    for profile, _value in fractions[:remainder]:
        counts[profile.name] += 1
    return [
        profile.name
        for profile in profiles
        for _ in range(counts[profile.name])
    ]


def generated_fixture_path(
    index: int,
    kind: str,
    size: int,
    max_depth: int,
    rng: random.Random,
    profile_name: str,
) -> Path:
    profile = profile_for_name(profile_name, kind)
    dirs = list(rng.choice(profile.directories))
    if max_depth > 0:
        dirs = dirs[:max_depth]
    if not dirs:
        dirs = [f"group-{rng.randrange(64):02x}"]
    filename = f"{profile.stem}-{index:06d}-{size}{profile.extension}"
    return Path(*dirs, filename)


def profile_for_name(profile_name: str, kind: str) -> ContentProfile:
    profile = PROFILE_BY_NAME.get(profile_name)
    if profile is None or PROFILE_KIND_BY_NAME[profile.name] != kind:
        raise FixtureContentError(f"unsupported {kind} profile: {profile_name}")
    return profile


def profile_for_manifest_entry(kind: str, profile_name: str | None, path: str) -> str:
    if profile_name:
        profile = PROFILE_BY_NAME.get(profile_name)
        if profile is not None and PROFILE_KIND_BY_NAME[profile.name] == kind:
            return profile.name
    suffix = PurePosixPath(path).suffix.lower()
    for profile in PROFILE_GROUPS.get(kind, ()):
        if profile.extension == suffix:
            return profile.name
    profiles = PROFILE_GROUPS.get(kind)
    if not profiles:
        raise FixtureContentError(f"unsupported file kind: {kind}")
    return profiles[0].name


def stable_profile_salt(profile_name: str) -> int:
    return sum((index + 1) * ord(char) for index, char in enumerate(profile_name))


def random_bytes(rng: random.Random, length: int) -> bytes:
    if hasattr(rng, "randbytes"):
        return rng.randbytes(length)
    return rng.getrandbits(length * 8).to_bytes(length, "little")


class FixtureContentStream:
    def __init__(
        self,
        *,
        seed: int,
        index: int,
        kind: str,
        profile: str,
        mutation: bool = False,
    ) -> None:
        profile_for_name(profile, kind)
        self.seed = seed
        self.index = index
        self.kind = kind
        self.profile = profile
        self.mutation = mutation
        self.offset = 0
        self.record = 0
        self.pending = bytearray()
        salt = stable_profile_salt(profile)
        variant = 0xA5A5 if mutation else 0x5EED
        self.text_rng = random.Random((seed << 48) ^ (index << 8) ^ salt ^ variant)
        self.random_rng = random.Random(
            (seed << 40) ^ (index << 16) ^ salt ^ variant ^ 0xBEEF
        )

    def next_bytes(self, length: int) -> bytes:
        if self.kind == "incompressible":
            self.offset += length
            return random_bytes(self.random_rng, length)
        if self.kind == "compressible":
            data = self._structured(length)
            self.offset += length
            return data
        data = self._mixed(length)
        self.offset += length
        return data

    def _structured(self, length: int) -> bytes:
        while len(self.pending) < length:
            self.pending.extend(
                render_record(
                    profile=self.profile,
                    index=self.index,
                    record=self.record,
                    rng=self.text_rng,
                    mutation=self.mutation,
                ).encode()
            )
            self.record += 1
        data = bytes(self.pending[:length])
        del self.pending[:length]
        return data

    def _mixed(self, length: int) -> bytes:
        data = bytearray()
        while len(data) < length:
            absolute = self.offset + len(data)
            block_remaining = 8192 - (absolute % 8192)
            want = min(block_remaining, length - len(data))
            block_index = absolute // 8192
            if block_index % 4 == 3:
                data.extend(random_bytes(self.random_rng, want))
            else:
                data.extend(self._structured(want))
        return bytes(data)


def render_record(
    *,
    profile: str,
    index: int,
    record: int,
    rng: random.Random,
    mutation: bool,
) -> str:
    if profile == "markdown":
        return render_markdown(index, record, rng, mutation)
    if profile in {"typescript", "bundled-js"}:
        return render_typescript(index, record, rng, mutation)
    if profile == "javascript":
        return render_javascript(index, record, rng, mutation)
    if profile in {"json", "source-map"}:
        return render_json(index, record, rng, mutation)
    if profile == "rust":
        return render_rust(index, record, rng, mutation)
    if profile == "css":
        return render_css(index, record, rng, mutation)
    if profile == "html":
        return render_html(index, record, rng, mutation)
    if profile == "python":
        return render_python(index, record, rng, mutation)
    if profile == "yaml":
        return render_yaml(index, record, rng, mutation)
    if profile == "toml":
        return render_toml(index, record, rng, mutation)
    return render_log(index, record, rng, mutation)


def render_markdown(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    if record == 0:
        return (
            f"# Deployment fixture {index:06d}\n\n"
            "This synthetic document models a real repository note with repeated "
            "terms, varied identifiers, and small code excerpts.\n\n"
        )
    topic = choice(rng, WORDS)
    if record % 11 == 0:
        return f"## {topic.title()} checklist {record}\n\n"
    if record % 7 == 0:
        return (
            "```ts\n"
            f"const artifact{record} = await loadArtifact(\"{hex_token(rng, 8)}\");\n"
            f"expect(artifact{record}.route).toBe(\"{choice(rng, ROUTES)}\");\n"
            "```\n\n"
        )
    return (
        f"- `{topic}` updates `{choice(rng, ROUTES)}` for owner `{choice(rng, OWNERS)}` "
        f"with revision `{hex_token(rng, 10)}` and status `{status(mutation)}`.\n"
    )


def render_typescript(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    name = camel(choice(rng, WORDS), record)
    return (
        f"export async function {name}(ctx: RequestContext): Promise<RouteResult> {{\n"
        f"  const checksum = \"{hex_token(rng, 16)}\";\n"
        f"  const route = \"{choice(rng, ROUTES)}\";\n"
        "  ctx.logger.info({ "
        f"route, checksum, fixture: {index}, status: \"{status(mutation)}\" "
        "});\n"
        "  return { route, checksum, ok: true };\n"
        "}\n\n"
    )


def render_javascript(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    return (
        f"const route{record} = \"{choice(rng, ROUTES)}\";\n"
        f"export const task{index}_{record} = {{\n"
        f"  owner: \"{choice(rng, OWNERS)}\",\n"
        f"  hash: \"{hex_token(rng, 12)}\",\n"
        f"  status: \"{status(mutation)}\",\n"
        f"  load: () => fetch(route{record}).then((r) => r.status),\n"
        "};\n\n"
    )


def render_json(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    return (
        "{"
        f"\"fixture\":{index},"
        f"\"record\":{record},"
        f"\"route\":\"{choice(rng, ROUTES)}\","
        f"\"owner\":\"{choice(rng, OWNERS)}\","
        f"\"status\":\"{status(mutation)}\","
        f"\"checksum\":\"{hex_token(rng, 18)}\","
        f"\"labels\":[\"{choice(rng, WORDS)}\",\"{choice(rng, WORDS)}\"]"
        "}\n"
    )


def render_rust(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    name = snake(choice(rng, WORDS), record)
    return (
        f"pub fn {name}() -> FixtureRecord {{\n"
        f"    FixtureRecord::new({index}, {record})\n"
        f"        .with_route(\"{choice(rng, ROUTES)}\")\n"
        f"        .with_checksum(\"{hex_token(rng, 16)}\")\n"
        f"        .with_status(\"{status(mutation)}\")\n"
        "}\n\n"
    )


def render_css(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    color = hex_token(rng, 3)
    return (
        f".fixture-{index}-{record} {{\n"
        f"  --accent: #{color};\n"
        f"  color: #{hex_token(rng, 3)};\n"
        f"  background: linear-gradient(90deg, #{color}, #{hex_token(rng, 3)});\n"
        f"  content: \"{status(mutation)} {choice(rng, WORDS)}\";\n"
        "}\n\n"
    )


def render_html(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    topic = choice(rng, WORDS)
    return (
        f"<section data-fixture=\"{index}\" data-record=\"{record}\">\n"
        f"  <h2>{topic.title()} {record}</h2>\n"
        f"  <a href=\"{choice(rng, ROUTES)}\">{choice(rng, OWNERS)}</a>\n"
        f"  <code>{hex_token(rng, 14)}</code>\n"
        f"  <p>Status: {status(mutation)}</p>\n"
        "</section>\n"
    )


def render_python(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    name = snake(choice(rng, WORDS), record)
    return (
        f"def {name}(client):\n"
        f"    checksum = \"{hex_token(rng, 16)}\"\n"
        f"    route = \"{choice(rng, ROUTES)}\"\n"
        "    client.emit("
        f"route=route, checksum=checksum, fixture={index}, status=\"{status(mutation)}\""
        ")\n"
        "    return checksum\n\n"
    )


def render_yaml(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    return (
        f"job_{index}_{record}:\n"
        f"  owner: {choice(rng, OWNERS)}\n"
        f"  route: {choice(rng, ROUTES)}\n"
        f"  checksum: {hex_token(rng, 16)}\n"
        f"  status: {status(mutation)}\n"
    )


def render_toml(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    return (
        f"[fixture.{index}.{record}]\n"
        f"owner = \"{choice(rng, OWNERS)}\"\n"
        f"route = \"{choice(rng, ROUTES)}\"\n"
        f"checksum = \"{hex_token(rng, 16)}\"\n"
        f"status = \"{status(mutation)}\"\n\n"
    )


def render_log(index: int, record: int, rng: random.Random, mutation: bool) -> str:
    return (
        f"ts=2026-05-02T{record % 24:02d}:{record % 60:02d}:00Z "
        f"level=info fixture={index} record={record} owner={choice(rng, OWNERS)} "
        f"route={choice(rng, ROUTES)} checksum={hex_token(rng, 16)} status={status(mutation)}\n"
    )


def choice(rng: random.Random, values: tuple[str, ...]) -> str:
    return values[rng.randrange(len(values))]


def hex_token(rng: random.Random, bytes_count: int) -> str:
    return "".join(f"{rng.randrange(256):02x}" for _ in range(bytes_count))


def status(mutation: bool) -> str:
    return "updated" if mutation else "ok"


def camel(word: str, record: int) -> str:
    return "".join(part.title() for part in word.split("-")) + f"{record}"


def snake(word: str, record: int) -> str:
    return word.replace("-", "_") + f"_{record}"


class FixtureContentError(Exception):
    pass
