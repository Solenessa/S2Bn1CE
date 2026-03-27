#!/usr/bin/env python3

from __future__ import annotations

import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
DEFAULT_DB_PATH = DATA_DIR / "sims2_cc.db"
SCHEMA_PATH = APP_DIR / "schema.sql"
WEB_DB_ENV_VAR = "SIMS2_CC_DB_PATH"


def resolve_db_path(db_path: Path | None = None) -> Path:
    return (db_path or DEFAULT_DB_PATH).expanduser().resolve()


def resolve_web_db_path(db_path: Path | None = None) -> Path:
    configured = db_path
    if configured is None:
        env_value = os.environ.get(WEB_DB_ENV_VAR, "").strip()
        if env_value:
            configured = Path(env_value)
    return resolve_db_path(configured)


def connect(db_path: Path | None = None) -> sqlite3.Connection:
    connection = sqlite3.connect(resolve_db_path(db_path))
    connection.execute("PRAGMA foreign_keys = ON")
    connection.row_factory = sqlite3.Row
    return connection


def initialize_database(db_path: Path | None = None) -> Path:
    try:
        from app.migrations import migrate_database
    except ModuleNotFoundError:
        from migrations import migrate_database

    resolved = resolve_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    connection = connect(resolved)
    connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    migrate_database(connection)
    connection.commit()
    connection.close()
    return resolved


def backup_database(db_path: Path | None = None) -> Path:
    resolved = resolve_db_path(db_path)
    if not resolved.exists():
        raise FileNotFoundError(f"Database does not exist: {resolved}")
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = resolved.with_name(f"{resolved.stem}.backup-{timestamp}{resolved.suffix}")
    shutil.copy2(resolved, backup_path)
    return backup_path
