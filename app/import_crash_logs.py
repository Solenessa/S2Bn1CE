#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

try:
    from app.db import connect, resolve_db_path
    from app.crash_parser import parse_crash_log
except ModuleNotFoundError:
    from db import connect, resolve_db_path
    from crash_parser import parse_crash_log


DB_PATH = resolve_db_path()


LOG_EXTENSIONS = {".txt", ".log"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Sims 2 crash logs and config logs into the diagnostics database.")
    parser.add_argument("--root", type=Path, required=True, help="Root folder containing crash and config logs")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    return parser.parse_args()


def import_logs(root: Path, db_path: Path = DB_PATH) -> dict[str, int | str]:
    root = root.expanduser().resolve()
    db_path = resolve_db_path(db_path)
    if not root.exists():
        raise FileNotFoundError(f"Root does not exist: {root}")
    if not db_path.exists():
        raise FileNotFoundError(f"Database does not exist: {db_path}. Run bootstrap_db.py first.")

    connection = connect(db_path)

    imported = 0
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in LOG_EXTENSIONS:
            continue
        parsed = parse_crash_log(path)
        connection.execute(
            """
            INSERT INTO crash_reports (
              source_path, file_name, log_type, sha256, occurred_at_text, app_name,
              exception_code, exception_module, fault_address, crash_category, summary,
              graphics_device, graphics_vendor, driver_version, texture_memory_mb,
              os_version, memory_hint, raw_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_path) DO UPDATE SET
              file_name = excluded.file_name,
              log_type = excluded.log_type,
              sha256 = excluded.sha256,
              occurred_at_text = excluded.occurred_at_text,
              app_name = excluded.app_name,
              exception_code = excluded.exception_code,
              exception_module = excluded.exception_module,
              fault_address = excluded.fault_address,
              crash_category = excluded.crash_category,
              summary = excluded.summary,
              graphics_device = excluded.graphics_device,
              graphics_vendor = excluded.graphics_vendor,
              driver_version = excluded.driver_version,
              texture_memory_mb = excluded.texture_memory_mb,
              os_version = excluded.os_version,
              memory_hint = excluded.memory_hint,
              raw_text = excluded.raw_text,
              imported_at = CURRENT_TIMESTAMP
            """,
            (
                parsed.source_path,
                parsed.file_name,
                parsed.log_type,
                parsed.sha256,
                parsed.occurred_at_text,
                parsed.app_name,
                parsed.exception_code,
                parsed.exception_module,
                parsed.fault_address,
                parsed.crash_category,
                parsed.summary,
                parsed.graphics_device,
                parsed.graphics_vendor,
                parsed.driver_version,
                parsed.texture_memory_mb,
                parsed.os_version,
                parsed.memory_hint,
                parsed.raw_text,
            ),
        )
        imported += 1

    connection.commit()
    crash_count = connection.execute("SELECT COUNT(*) FROM crash_reports WHERE log_type = 'crash'").fetchone()[0]
    config_count = connection.execute("SELECT COUNT(*) FROM crash_reports WHERE log_type = 'config'").fetchone()[0]
    connection.close()

    return {
        "root": str(root),
        "imported": imported,
        "crash_count": crash_count,
        "config_count": config_count,
    }


def main() -> None:
    args = parse_args()
    result = import_logs(args.root, args.db)
    print(f"Imported {result['imported']} log files from {result['root']}")
    print(f"Crash reports stored: {result['crash_count']}")
    print(f"Config logs stored: {result['config_count']}")


if __name__ == "__main__":
    main()
