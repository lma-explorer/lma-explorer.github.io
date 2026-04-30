"""Tests that every committed manifest in data/processed/ has the
expected schema. Manifests are append-only and the methodology page
+ data catalog read them at render time; if a manifest is malformed,
the page either shows '(no manifest yet)' or crashes the render.

These tests are file-system-level and run against whatever is in the
working tree at test time. They're cheap and they catch the
'someone added a vintage but forgot a required field' regression.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED = REPO_ROOT / "data" / "processed"

# Required keys on each manifest entry. Pinned narrowly: a manifest with
# extra keys is fine; a manifest missing any of these breaks the methodology
# page's freshness table and the data catalog's per-snapshot block.
REQUIRED_ENTRY_KEYS = {"vintage", "sha256", "written_at_utc"}


def _manifest_paths() -> list[Path]:
    if not PROCESSED.exists():
        return []
    return sorted(PROCESSED.glob("*MANIFEST*.json"))


@pytest.mark.parametrize("path", _manifest_paths(), ids=lambda p: p.name)
def test_manifest_is_valid_json(path: Path) -> None:
    """Every manifest must be parseable JSON. A render crashes hard
    otherwise."""
    with path.open("r", encoding="utf-8") as f:
        json.load(f)  # raises if invalid


@pytest.mark.parametrize("path", _manifest_paths(), ids=lambda p: p.name)
def test_manifest_has_entries_list(path: Path) -> None:
    """Every manifest is shaped {entries: [...]}. The methodology page
    indexes -1 to get the latest vintage."""
    with path.open("r", encoding="utf-8") as f:
        m = json.load(f)
    assert "entries" in m, f"{path.name} missing 'entries' key"
    assert isinstance(m["entries"], list), f"{path.name} 'entries' must be a list"


@pytest.mark.parametrize("path", _manifest_paths(), ids=lambda p: p.name)
def test_manifest_entries_have_required_keys(path: Path) -> None:
    """Every entry must have vintage, sha256, written_at_utc — the three
    fields the data catalog and the methodology freshness table read."""
    with path.open("r", encoding="utf-8") as f:
        m = json.load(f)
    entries = m.get("entries", [])
    if not entries:
        pytest.skip(f"{path.name} has no entries yet — skipping required-key check")
    for i, entry in enumerate(entries):
        missing = REQUIRED_ENTRY_KEYS - set(entry.keys())
        assert not missing, (
            f"{path.name}[{i}] missing required keys: {missing}"
        )


@pytest.mark.parametrize("path", _manifest_paths(), ids=lambda p: p.name)
def test_manifest_last_entry_is_latest_vintage(path: Path) -> None:
    """The methodology freshness table and the data catalog both read
    `entries[-1]` and display it as the "latest vintage." This invariant
    is what makes that work: entries[-1] should have the lexicographically
    maximum vintage in the manifest. (Entries CAN be out of order in the
    file — backfills happen — but the most-recent-vintage entry must be
    last.)"""
    with path.open("r", encoding="utf-8") as f:
        m = json.load(f)
    entries = m.get("entries", [])
    if len(entries) < 2:
        pytest.skip(f"{path.name} has fewer than 2 entries")
    vintages = [str(e.get("vintage", "")) for e in entries]
    assert vintages[-1] == max(vintages), (
        f"{path.name} entries[-1] vintage {vintages[-1]!r} is not the max "
        f"({max(vintages)!r}). The methodology page would misreport "
        f"this manifest's latest vintage."
    )
