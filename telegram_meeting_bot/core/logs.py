from __future__ import annotations

import logging
import re
import tempfile
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, List, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

from .constants import LOGS_APP_DIR, LOGS_AUDIT_DIR, LOGS_ERROR_DIR

LOG_TYPE_APP = "app"
LOG_TYPE_AUDIT = "audit"
LOG_TYPE_ERROR = "error"

_LOG_SOURCES: dict[str, Tuple[Path, str]] = {
    LOG_TYPE_APP: (LOGS_APP_DIR, "app"),
    LOG_TYPE_AUDIT: (LOGS_AUDIT_DIR, "audit"),
    LOG_TYPE_ERROR: (LOGS_ERROR_DIR, "error"),
}

_PREVIEW_LIMIT_DEFAULT = 12
_APP_ENTRY_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b")
_EPOCH = datetime.min.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class LogFileInfo:
    """Metadata about a stored log file."""

    log_type: str
    path: Path
    label: str
    size_bytes: int
    modified_at: datetime | None

    @property
    def name(self) -> str:
        return self.path.name


@dataclass(frozen=True)
class LogFileView:
    """Result of reading log entries from a file."""

    entries: List[List[str]]
    total: int
    truncated: bool


class ErrorBurstHandler(logging.Handler):
    """Trigger callback when too many error records appear in a short time."""

    def __init__(
        self,
        *,
        threshold: int = 5,
        window_seconds: float = 60.0,
        cooldown_seconds: float = 300.0,
    ) -> None:
        super().__init__(level=logging.ERROR)
        self.threshold = max(1, threshold)
        self.window_seconds = max(1.0, window_seconds)
        self.cooldown_seconds = max(0.0, cooldown_seconds)
        self._recent: deque[float] = deque()
        self._callback: Callable[[logging.LogRecord, int], None] | None = None
        self._last_alert: float = 0.0

    def set_callback(self, callback: Callable[[logging.LogRecord, int], None] | None) -> None:
        self._callback = callback

    def reset(self) -> None:
        self._recent.clear()
        self._last_alert = 0.0

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - exercised indirectly
        if record.levelno < logging.ERROR:
            return
        now = time.monotonic()
        self._recent.append(now)
        while self._recent and now - self._recent[0] > self.window_seconds:
            self._recent.popleft()
        count = len(self._recent)
        if count < self.threshold:
            return
        if self.cooldown_seconds and now - self._last_alert < self.cooldown_seconds:
            return
        if not self._callback:
            return
        self._last_alert = now
        try:
            self._callback(record, count)
        except Exception:  # pragma: no cover - defensive
            logging.getLogger(__name__).exception("Error burst callback failed")


ERROR_BURST_MONITOR = ErrorBurstHandler()


def set_error_burst_callback(callback: Callable[[logging.LogRecord, int], None] | None) -> None:
    """Register callback that will be invoked on error burst."""

    ERROR_BURST_MONITOR.set_callback(callback)


def iter_log_files(log_type: str | None = None) -> Iterator[Tuple[str, Path]]:
    """Yield known log files grouped by type."""

    sources: Iterable[Tuple[str, Tuple[Path, str]]]
    if log_type:
        key = log_type.lower()
        if key not in _LOG_SOURCES:
            raise ValueError(f"Unknown log type: {log_type}")
        sources = ((key, _LOG_SOURCES[key]),)
    else:
        sources = _LOG_SOURCES.items()

    for kind, (directory, prefix) in sources:
        try:
            paths = sorted(directory.glob(f"{prefix}_*.log"))
        except FileNotFoundError:
            continue
        for path in paths:
            if path.is_file():
                yield kind, path


def list_log_files(log_type: str) -> List[LogFileInfo]:
    """Return metadata for available files of the given log type."""

    kind = log_type.lower()
    if kind not in _LOG_SOURCES:
        raise ValueError(f"Unknown log type: {log_type}")
    directory, prefix = _LOG_SOURCES[kind]
    try:
        paths = list(directory.glob(f"{prefix}_*.log"))
    except FileNotFoundError:
        return []

    infos: List[LogFileInfo] = []
    for path in paths:
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            size = 0
            modified = None
        else:
            size = stat.st_size
            modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        label = path.name
        suffix = ".log"
        prefix_token = f"{prefix}_"
        if label.startswith(prefix_token) and label.endswith(suffix):
            label = label[len(prefix_token) : -len(suffix)]
        infos.append(
            LogFileInfo(
                log_type=kind,
                path=path,
                label=label,
                size_bytes=size,
                modified_at=modified,
            )
        )
    infos.sort(key=lambda info: (info.modified_at or _EPOCH, info.name), reverse=True)
    return infos


def get_recent_entries(log_type: str, limit: int = _PREVIEW_LIMIT_DEFAULT) -> List[str]:
    """Return last ``limit`` log lines for the specified log type."""

    limit = max(1, limit)
    kind = log_type.lower()
    if kind not in _LOG_SOURCES:
        raise ValueError(f"Unknown log type: {log_type}")
    directory, prefix = _LOG_SOURCES[kind]
    lines: deque[str] = deque(maxlen=limit)
    try:
        paths = sorted(directory.glob(f"{prefix}_*.log"))
    except FileNotFoundError:
        return []
    for path in paths:
        try:
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                for raw in fh:
                    lines.append(raw.rstrip("\n"))
        except OSError:
            continue
    return list(lines)


def get_log_file_info(log_type: str, file_name: str) -> LogFileInfo:
    """Return metadata for a specific log file name."""

    kind = log_type.lower()
    if kind not in _LOG_SOURCES:
        raise ValueError(f"Unknown log type: {log_type}")
    directory, _ = _LOG_SOURCES[kind]
    path = (directory / file_name).resolve()
    try:
        directory_resolved = directory.resolve()
    except FileNotFoundError:
        directory_resolved = directory
    if path.parent != directory_resolved:
        raise FileNotFoundError(file_name)
    if not path.is_file():
        raise FileNotFoundError(file_name)

    try:
        stat = path.stat()
    except OSError:
        size = 0
        modified = None
    else:
        size = stat.st_size
        modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)

    label = file_name
    suffix = ".log"
    prefix = f"{_LOG_SOURCES[kind][1]}_"
    if label.startswith(prefix) and label.endswith(suffix):
        label = label[len(prefix) : -len(suffix)]

    return LogFileInfo(
        log_type=kind,
        path=path,
        label=label,
        size_bytes=size,
        modified_at=modified,
    )


def read_log_entries(
    log_type: str,
    path: Path,
    *,
    limit: int | None = None,
) -> LogFileView:
    """Read entries from a log file grouped by record boundaries."""

    kind = log_type.lower()
    if kind not in _LOG_SOURCES:
        raise ValueError(f"Unknown log type: {log_type}")
    entries_container: deque[List[str]] | List[List[str]]
    if limit is not None:
        entries_container = deque(maxlen=max(1, limit))
    else:
        entries_container = []
    total = 0

    def _append(entry: List[str]) -> None:
        if isinstance(entries_container, deque):
            entries_container.append(entry)
        else:
            entries_container.append(entry)

    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            if kind == LOG_TYPE_APP:
                current: List[str] = []
                for raw in fh:
                    line = raw.rstrip("\n")
                    if _APP_ENTRY_RE.match(line):
                        if current:
                            _append(current)
                            total += 1
                        current = [line]
                    else:
                        if current:
                            current.append(line)
                        elif line:
                            current = [line]
                if current:
                    _append(current)
                    total += 1
            else:
                for raw in fh:
                    line = raw.rstrip("\n")
                    if not line:
                        continue
                    _append([line])
                    total += 1
    except FileNotFoundError:
        return LogFileView(entries=[], total=0, truncated=False)

    if isinstance(entries_container, deque):
        entries_list = list(entries_container)
        truncated = total > len(entries_list)
    else:
        entries_list = list(entries_container)
        truncated = False
    return LogFileView(entries=entries_list, total=total, truncated=truncated)


def build_logs_archive() -> Path:
    """Create ZIP archive with all log files and return its path."""

    tmp_dir = Path(tempfile.gettempdir())
    archive = tmp_dir / f"bot-logs-{uuid.uuid4().hex[:8]}.zip"
    with ZipFile(archive, "w", compression=ZIP_DEFLATED) as zf:
        added = False
        for kind, path in iter_log_files():
            try:
                zf.write(path, arcname=f"{kind}/{path.name}")
                added = True
            except OSError:
                continue
        if not added:
            info = "Логи отсутствуют."
            zf.writestr("README.txt", info)
    return archive


def clear_all_logs() -> int:
    """Remove rotated files and truncate current log files.

    Returns number of files affected.
    """

    affected = 0
    for kind, (directory, prefix) in _LOG_SOURCES.items():
        try:
            paths = sorted(directory.glob(f"{prefix}_*.log"))
        except FileNotFoundError:
            continue
        if not paths:
            continue
        old = paths[:-1]
        current = paths[-1]
        for path in old:
            try:
                path.unlink()
                affected += 1
            except OSError:
                continue
        try:
            with current.open("w", encoding="utf-8"):
                affected += 1
        except OSError:
            continue
    ERROR_BURST_MONITOR.reset()
    return affected


def describe_log_type(log_type: str) -> str:
    labels = {
        LOG_TYPE_APP: "App",
        LOG_TYPE_AUDIT: "Audit",
        LOG_TYPE_ERROR: "Error",
    }
    key = log_type.lower()
    if key not in labels:
        raise ValueError(f"Unknown log type: {log_type}")
    return labels[key]
