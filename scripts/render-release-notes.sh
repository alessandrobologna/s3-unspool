#!/usr/bin/env bash
set -euo pipefail

require_env() {
  name="$1"
  if [ -z "${!name:-}" ]; then
    echo "::error title=missing environment::${name} must be set." >&2
    exit 2
  fi
}

require_env ANNOUNCEMENT_BODY
require_env RELEASE_COMMIT
require_env TAG

export TAG

output_path="${OUTPUT_PATH:-${RUNNER_TEMP:-/tmp}/notes.md}"
version="${TAG#v}"
repository="${GITHUB_REPOSITORY:-alessandrobologna/s3-unspool}"
server_url="${GITHUB_SERVER_URL:-https://github.com}"

git fetch --tags --force origin >/dev/null

previous_tag="$(
  gh release list \
    --limit 50 \
    --json tagName,isDraft \
    --jq '[.[] | select(.isDraft == false and .tagName != env.TAG)][0].tagName // ""'
)"

render_commits() {
  if [ -n "$previous_tag" ]; then
    echo "Since ${previous_tag}:"
    echo
    commits="$(git log --first-parent --reverse --pretty=format:'- %s (%h)' "${previous_tag}..${RELEASE_COMMIT}")"
    if [ -n "$commits" ]; then
      printf '%s\n' "$commits"
    else
      echo "- No first-parent commits since ${previous_tag}."
    fi
    echo
    echo "[Full diff](${server_url}/${repository}/compare/${previous_tag}...${TAG})"
    return
  fi

  echo "Initial release commits:"
  echo
  git log --first-parent --reverse --pretty=format:'- %s (%h)' "$RELEASE_COMMIT"
  echo
}

{
  echo "## Install CLI"
  echo
  echo '```sh'
  echo "cargo binstall s3-unspool-cli@${version}"
  echo '```'
  echo
  printf '%s\n\n' "$ANNOUNCEMENT_BODY"
  echo "## Commits"
  echo
  render_commits
} > "$output_path"
