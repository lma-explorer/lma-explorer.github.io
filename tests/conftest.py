"""Pytest configuration. Adds the repo root to sys.path so tests can
import `pipelines.*` without an editable install.

Tests are deliberately narrow: each one targets a single pure-function
behavior and uses synthetic in-memory inputs (no parquet reads, no API
calls, no network). The point is to catch regressions in math and
filter logic, not to re-test pandas itself.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _add_repo_root_to_syspath() -> None:
    here = Path(__file__).resolve().parent
    repo_root = here.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


_add_repo_root_to_syspath()
