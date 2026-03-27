#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

try:
    from app.db import backup_database, initialize_database, resolve_db_path
except ModuleNotFoundError:
    from db import backup_database, initialize_database, resolve_db_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize or reset the Sims 2 CC diagnostics database.")
    parser.add_argument("--db", type=Path, default=resolve_db_path())
    parser.add_argument("--reset", action="store_true", help="Backup and recreate the database file")
    parser.add_argument("--yes", action="store_true", help="Required with --reset to confirm destructive recreation")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = resolve_db_path(args.db)
    if args.reset:
        if not args.yes:
            raise SystemExit("--reset requires --yes because it recreates the database file after taking a backup.")
        backup_path = None
        if db_path.exists():
            backup_path = backup_database(db_path)
            db_path.unlink()
        initialized_path = initialize_database(db_path)
        if backup_path:
            print(f"Backed up existing database to {backup_path}")
        print(f"Recreated database at {initialized_path}")
        return
    initialized_path = initialize_database(db_path)
    print(f"Initialized database at {initialized_path}")


if __name__ == "__main__":
    main()
