from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from zipfile import ZIP_DEFLATED, ZipFile

from .core import feature_flags, release_history, storage
from .core.constants import DATA_DIR, LOGS_DIR, OWNER_USERNAMES, VERSION


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _print_json(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _iter_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return [p for p in root.rglob("*") if p.is_file()]


def cmd_status(_: argparse.Namespace) -> int:
    revision = release_history.current_revision()
    jobs = storage.get_jobs_store()
    chats = storage.get_known_chats()
    payload = {
        "ts": _iso_now(),
        "version": VERSION,
        "revision": revision,
        "data_dir": str(DATA_DIR),
        "jobs_total": len(jobs),
        "chats_total": len(chats),
        "owners": sorted(list(OWNER_USERNAMES)),
        "owner_ids_known": sorted(list(storage.get_owner_user_ids())),
        "flags": feature_flags.list_flags(),
    }
    _print_json(payload)
    return 0


def cmd_backup(args: argparse.Namespace) -> int:
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else (DATA_DIR / "backups")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    archive_path = out_dir / f"bot-backup-{stamp}.zip"

    files: list[Path] = []
    files.extend(_iter_files(DATA_DIR))
    if args.include_logs:
        files.extend(_iter_files(LOGS_DIR))

    unique: dict[str, Path] = {}
    for path in files:
        try:
            rel = path.resolve().relative_to(DATA_DIR.resolve())
            arcname = Path("data") / rel
        except Exception:
            try:
                rel = path.resolve().relative_to(LOGS_DIR.resolve())
                arcname = Path("data") / "logs" / rel
            except Exception:
                continue
        unique[str(arcname)] = path

    with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as zf:
        manifest = {
            "ts": _iso_now(),
            "version": VERSION,
            "revision": release_history.current_revision(),
            "files": sorted(unique.keys()),
        }
        zf.writestr("MANIFEST.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        for arcname, path in sorted(unique.items()):
            zf.write(path, arcname=arcname)

    print(str(archive_path))
    return 0


def cmd_verify_db(_: argparse.Namespace) -> int:
    jobs = storage.get_jobs_store()
    issues: list[dict[str, object]] = []

    for rec in jobs:
        job_id = rec.get("job_id")
        run_at = rec.get("run_at_utc")
        if not isinstance(run_at, str) or not run_at:
            issues.append({"job_id": job_id, "issue": "missing_run_at_utc"})
        else:
            try:
                datetime.fromisoformat(run_at)
            except ValueError:
                issues.append({"job_id": job_id, "issue": "invalid_run_at_utc", "value": run_at})

        signature = rec.get("signature")
        if not isinstance(signature, str) or not signature:
            issues.append({"job_id": job_id, "issue": "missing_signature"})

    payload = {
        "ts": _iso_now(),
        "jobs_total": len(jobs),
        "issues_total": len(issues),
        "issues": issues,
    }
    _print_json(payload)
    return 0 if not issues else 2


def cmd_compact_chats(_: argparse.Namespace) -> int:
    before = len(storage.get_known_chats())
    removed = storage.compact_known_chats_by_chat_id()
    after = len(storage.get_known_chats())
    _print_json({"before": before, "after": after, "removed": removed})
    return 0


def cmd_history(args: argparse.Namespace) -> int:
    items = release_history.get_history(limit=max(1, int(args.limit)))
    _print_json({"count": len(items), "items": items})
    return 0


def cmd_flags(_: argparse.Namespace) -> int:
    _print_json(feature_flags.list_flags())
    return 0


def cmd_set_flag(args: argparse.Namespace) -> int:
    value = args.value.strip().lower()
    if value not in {"true", "false", "1", "0", "on", "off"}:
        print("Value must be true/false", file=sys.stderr)
        return 2
    bool_value = value in {"true", "1", "on"}
    feature_flags.set_flag(args.name, bool_value)
    _print_json({"name": args.name, "value": bool_value})
    return 0


def cmd_restore(args: argparse.Namespace) -> int:
    if not args.force:
        print("Restore is destructive. Re-run with --force.", file=sys.stderr)
        return 2

    archive = Path(args.archive).expanduser().resolve()
    if not archive.is_file():
        print(f"Archive not found: {archive}", file=sys.stderr)
        return 2

    with ZipFile(archive, "r") as zf:
        members = [m for m in zf.namelist() if m.startswith("data/") and not m.endswith("/")]
        for member in members:
            rel = Path(member).relative_to("data")
            target = (DATA_DIR / rel).resolve()
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(member) as src, target.open("wb") as dst:
                dst.write(src.read())

    _print_json({"restored_from": str(archive), "files": len(members)})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m telegram_meeting_bot.admin_cli",
        description="Administrative CLI for Telegram Meeting Bot",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_status = sub.add_parser("status", help="Show runtime status")
    p_status.set_defaults(func=cmd_status)

    p_backup = sub.add_parser("backup", help="Create backup archive")
    p_backup.add_argument("--out-dir", default=None, help="Output directory for archive")
    p_backup.add_argument("--include-logs", action="store_true", help="Include log files")
    p_backup.set_defaults(func=cmd_backup)

    p_verify = sub.add_parser("verify-db", help="Validate reminders database records")
    p_verify.set_defaults(func=cmd_verify_db)

    p_compact = sub.add_parser("compact-chats", help="Compact known chats by chat_id")
    p_compact.set_defaults(func=cmd_compact_chats)

    p_history = sub.add_parser("history", help="Show startup revision history")
    p_history.add_argument("--limit", type=int, default=10)
    p_history.set_defaults(func=cmd_history)

    p_flags = sub.add_parser("flags", help="List feature flags")
    p_flags.set_defaults(func=cmd_flags)

    p_set_flag = sub.add_parser("set-flag", help="Set feature flag value")
    p_set_flag.add_argument("name")
    p_set_flag.add_argument("value")
    p_set_flag.set_defaults(func=cmd_set_flag)

    p_restore = sub.add_parser("restore", help="Restore backup archive into data dir")
    p_restore.add_argument("archive", help="Path to .zip backup")
    p_restore.add_argument("--force", action="store_true", help="Allow destructive restore")
    p_restore.set_defaults(func=cmd_restore)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
