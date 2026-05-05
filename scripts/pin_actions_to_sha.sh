#!/usr/bin/env bash
# pin_actions_to_sha.sh — convert tag-pinned GitHub Actions to SHA-pinned form.
#
# Why this exists: GitHub's secure-use reference recommends pinning
# third-party actions to a full-length commit SHA rather than a moving
# tag (https://docs.github.com/en/actions/security-for-github-actions/
# security-guides/security-hardening-for-github-actions).
#
# What it does: walks .github/workflows/*.yml, finds every
# `uses: <repo>[/subpath]@<tag>` line, resolves the tag to its current
# commit SHA via `gh api`, and rewrites the line to
# `uses: <repo>[/subpath]@<sha> # <tag>`. The trailing comment
# preserves the human-readable version for future reviewers.
#
# Subpath handling: actions like `quarto-dev/quarto-actions/setup@v2`
# tag the parent repo, not the subpath. The API call is made against
# just `<owner>/<repo>`; the rewrite preserves the full path-with-subpath.
#
# Validation: captured SHAs are checked against /^[0-9a-f]{40}$/ before
# use. If the API call returns an error JSON or any other non-SHA
# response, the affected line is skipped with a WARNING to stderr
# rather than corrupting the workflow file.
#
# Portability: written for bash 3.2+ (the macOS default). No
# associative arrays (declare -A), no Bash 4-only features.
# Memoization of repeated (repo, tag) lookups uses a temp file.
#
# Requirements: the GitHub CLI (`gh`) installed and authenticated.
#   - macOS: `brew install gh && gh auth login`
#   - Verify: `gh auth status` should show "Logged in to github.com"
#
# Usage (from repo root, on a clean working tree):
#   bash scripts/pin_actions_to_sha.sh
#
# The script prints the planned changes first and asks for
# confirmation before writing. Run with --apply to skip the prompt.
#
# Audit context: flagged in the 2026-05-04 independent audit (item #7).

set -euo pipefail

WORKFLOWS_DIR=".github/workflows"
APPLY=0
[ "${1:-}" = "--apply" ] && APPLY=1

if ! command -v gh >/dev/null 2>&1; then
  echo "ERROR: gh (GitHub CLI) not found on PATH." >&2
  echo "Install: brew install gh && gh auth login" >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "ERROR: gh is not authenticated. Run: gh auth login" >&2
  exit 1
fi

if [ ! -d "$WORKFLOWS_DIR" ]; then
  echo "ERROR: $WORKFLOWS_DIR not found. Run from repo root." >&2
  exit 1
fi

# is_sha — return 0 iff $1 is a 40-char lowercase hex string.
is_sha() {
  printf '%s' "$1" | grep -Eq '^[0-9a-f]{40}$'
}

# Temp files: SEEN_FILE memoizes (repo@tag) -> sha lookups; CHANGES_FILE
# accumulates the rewrite plan, one record per line.
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT
SEEN_FILE="$TMPDIR/seen.txt"
CHANGES_FILE="$TMPDIR/changes.txt"
: > "$SEEN_FILE"
: > "$CHANGES_FILE"

while IFS= read -r line; do
  file=$(printf '%s\n' "$line" | cut -d: -f1)
  lineno=$(printf '%s\n' "$line" | cut -d: -f2)
  content=$(printf '%s\n' "$line" | cut -d: -f3-)

  ref_part=$(printf '%s\n' "$content" | sed -n 's/^[[:space:]]*-\{0,1\}[[:space:]]*uses:[[:space:]]*\([^[:space:]]*\).*/\1/p')
  [ -z "$ref_part" ] && continue

  full_repo=${ref_part%@*}
  tag=${ref_part#*@}

  # Skip lines already pinned to a 40-char hex SHA.
  if is_sha "$tag"; then
    continue
  fi

  # API endpoint needs just <owner>/<name>. Strip subpath if present.
  api_repo=$(printf '%s' "$full_repo" | cut -d/ -f1,2)

  key="$full_repo@$tag"
  cached=$(grep -F "$key|" "$SEEN_FILE" 2>/dev/null | head -1 | cut -d'|' -f2 || true)

  if [ -n "$cached" ] && is_sha "$cached"; then
    sha="$cached"
  else
    # Try the lightweight tag-ref endpoint first.
    raw=$(gh api "repos/$api_repo/git/ref/tags/$tag" --jq '.object.sha' 2>/dev/null || true)
    sha=""
    if is_sha "$raw"; then
      sha="$raw"
    else
      # Fallback: commits endpoint accepts any ref (tag, branch, SHA).
      raw=$(gh api "repos/$api_repo/commits/$tag" --jq '.sha' 2>/dev/null || true)
      if is_sha "$raw"; then
        sha="$raw"
      fi
    fi

    if [ -z "$sha" ]; then
      printf 'WARNING: could not resolve %s — skipping\n' "$key" >&2
      continue
    fi

    # Dereference annotated-tag objects one level. If anything goes wrong
    # in this step, fall back to the original sha (still valid).
    obj_type=$(gh api "repos/$api_repo/git/objects/$sha" --jq '.type' 2>/dev/null || printf 'commit')
    if [ "$obj_type" = "tag" ]; then
      deref=$(gh api "repos/$api_repo/git/tags/$sha" --jq '.object.sha' 2>/dev/null || true)
      if is_sha "$deref"; then
        sha="$deref"
      fi
    fi

    printf '%s|%s\n' "$key" "$sha" >> "$SEEN_FILE"
  fi

  # One last sanity check before we trust this record.
  if ! is_sha "$sha"; then
    printf 'WARNING: %s resolved to non-SHA — skipping\n' "$key" >&2
    continue
  fi

  printf '%s|%s|%s|%s|%s\n' "$file" "$lineno" "$full_repo" "$tag" "$sha" >> "$CHANGES_FILE"
done < <(grep -nE '^[[:space:]]*-?[[:space:]]*uses:[[:space:]]+[^[:space:]]+@[^[:space:]]+' "$WORKFLOWS_DIR"/*.yml || true)

if [ ! -s "$CHANGES_FILE" ]; then
  echo "No tag-pinned actions found. All uses: directives are already on SHAs (or there are no workflows)."
  exit 0
fi

echo "Planned changes:"
echo
while IFS='|' read -r file lineno full_repo tag sha; do
  printf '  %s:%s  %s@%s  ->  %s@%s # %s\n' "$file" "$lineno" "$full_repo" "$tag" "$full_repo" "$sha" "$tag"
done < "$CHANGES_FILE"
echo

if [ $APPLY -eq 0 ]; then
  printf 'Apply these changes? [y/N] '
  read -r ans
  case "$ans" in
    [Yy]*) ;;
    *) echo "Aborted."; exit 0 ;;
  esac
fi

# Apply rewrites in place. sed -i.bak then remove the .bak for portability
# (BSD sed on macOS requires a suffix after -i; GNU sed accepts an empty arg).
while IFS='|' read -r file lineno full_repo tag sha; do
  sed -i.bak "${lineno}s|uses:[[:space:]]*${full_repo}@${tag}.*$|uses: ${full_repo}@${sha} # ${tag}|" "$file"
  rm -f "${file}.bak"
done < "$CHANGES_FILE"

echo
echo "Done. Review with 'git diff' before committing."
echo "Suggested branch: chore/pin-actions-to-sha"
echo "Suggested commit subject: chore(actions): pin third-party actions to full-length SHAs"
