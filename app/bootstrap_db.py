#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DB_PATH = DATA_DIR / "sims2_cc.db"
SCHEMA_PATH = APP_DIR / "schema.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or rebuild the Sims 2 CC diagnostics database.")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    return parser.parse_args()


def initialize_database(db_path: Path = DB_PATH) -> Path:
    db_path = db_path.expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.executescript(SCHEMA_PATH.read_text())
    connection.commit()
    connection.close()
    return db_path


def main() -> None:
    args = parse_args()
    db_path = initialize_database(args.db)
    print(f"Initialized database at {db_path}")


if __name__ == "__main__":
    main()
