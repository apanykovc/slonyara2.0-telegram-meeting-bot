from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List

from .constants import BASE_DIR, RELEASE_HISTORY_PATH, VERSION
from .storage import load_json, save_json


def _run_git(*args: str) -> str | None:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    out = (proc.stdout or "").strip()
    return out or None


def current_revision() -> Dict[str, Any]:
    commit = _run_git("rev-parse", "--short", "HEAD")
    branch = _run_git("rev-parse", "--abbrev-ref", "HEAD")
    dirty_raw = _run_git("status", "--porcelain")
    dirty = bool(dirty_raw)
    return {
        "version": VERSION,
        "commit": commit,
        "branch": branch,
        "dirty": dirty,
    }


def record_startup_revision(max_entries: int = 50) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rev = current_revision()
    entry: Dict[str, Any] = {"ts": now, **rev}

    raw = load_json(RELEASE_HISTORY_PATH, [])
    history: List[Dict[str, Any]] = [x for x in raw if isinstance(x, dict)] if isinstance(raw, list) else []

    if history and history[-1].get("commit") == entry.get("commit") and history[-1].get("dirty") == entry.get("dirty"):
        history[-1]["last_seen_ts"] = now
    else:
        history.append(entry)

    if len(history) > max_entries:
        history = history[-max_entries:]

    save_json(RELEASE_HISTORY_PATH, history)
    return entry


def get_history(limit: int = 10) -> List[Dict[str, Any]]:
    raw = load_json(RELEASE_HISTORY_PATH, [])
    history: List[Dict[str, Any]] = [x for x in raw if isinstance(x, dict)] if isinstance(raw, list) else []
    return history[-limit:]
