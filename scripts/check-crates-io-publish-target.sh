#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "::error title=missing crate names::usage: VERSION=<semver> $0 <crate>..." >&2
  exit 2
fi

if [ -z "${VERSION:-}" ]; then
  echo "::error title=missing version::VERSION must be set to the release version being published." >&2
  exit 2
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

user_agent="${CRATES_IO_USER_AGENT:-alessandrobologna/s3-unspool release workflow}"
failed=0

check_crate() {
  crate="$1"

  crate_status="$(curl -L -sS \
    --connect-timeout 10 \
    --max-time 30 \
    --retry 3 \
    --retry-delay 2 \
    --retry-all-errors \
    -o "$tmp_dir/${crate}.json" \
    -w "%{http_code}" \
    -H "User-Agent: ${user_agent}" \
    "https://crates.io/api/v1/crates/${crate}")"
  if [ "$crate_status" = "404" ]; then
    echo "::error title=crate bootstrap required::crates.io crate ${crate} does not exist. Trusted Publishing cannot create new crates; publish this crate once manually with an owner API token, configure Trusted Publishing for this workflow, then release a new version." >&2
    failed=1
    return
  fi
  if [ "$crate_status" != "200" ]; then
    cat "$tmp_dir/${crate}.json" >&2
    echo "::error title=unexpected crates.io response::got HTTP ${crate_status} while checking crate ${crate}." >&2
    failed=1
    return
  fi

  version_status="$(curl -L -sS \
    --connect-timeout 10 \
    --max-time 30 \
    --retry 3 \
    --retry-delay 2 \
    --retry-all-errors \
    -o "$tmp_dir/${crate}-${VERSION}.json" \
    -w "%{http_code}" \
    -H "User-Agent: ${user_agent}" \
    "https://crates.io/api/v1/crates/${crate}/${VERSION}")"
  if [ "$version_status" = "200" ]; then
    echo "::error title=crate version already published::crate ${crate} ${VERSION} is already published; bump the workspace version before rerunning the publish workflow." >&2
    failed=1
    return
  fi
  if [ "$version_status" != "404" ]; then
    cat "$tmp_dir/${crate}-${VERSION}.json" >&2
    echo "::error title=unexpected crates.io response::got HTTP ${version_status} while checking ${crate} ${VERSION}." >&2
    failed=1
  fi
}

for crate in "$@"; do
  check_crate "$crate"
done

if [ "$failed" -ne 0 ]; then
  exit 1
fi
