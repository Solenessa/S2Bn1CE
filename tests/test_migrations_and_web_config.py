from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.db import WEB_DB_ENV_VAR, connect, initialize_database
from app.migrations import CURRENT_SCHEMA_VERSION
from app.web_ui import create_app


class MigrationAndWebConfigTests(unittest.TestCase):
    def test_initialize_database_sets_schema_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "diag.db"
            initialize_database(db_path)
            connection = connect(db_path)
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            connection.close()
            self.assertEqual(version, CURRENT_SCHEMA_VERSION)

    def test_initialize_database_upgrades_older_database_without_destroying_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "legacy.db"
            connection = sqlite3.connect(db_path)
            connection.execute("CREATE TABLE scan_runs (id INTEGER PRIMARY KEY, root_path TEXT NOT NULL, started_at TEXT, finished_at TEXT)")
            connection.execute("INSERT INTO scan_runs (root_path) VALUES (?)", ("/legacy/root",))
            connection.execute("PRAGMA user_version = 0")
            connection.commit()
            connection.close()

            initialize_database(db_path)

            connection = connect(db_path)
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            count = connection.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0]
            scenegraph_exists = connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'scenegraph_names'"
            ).fetchone()[0]
            links_exists = connection.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'resource_links'"
            ).fetchone()[0]
            connection.close()
            self.assertEqual(version, CURRENT_SCHEMA_VERSION)
            self.assertEqual(count, 1)
            self.assertEqual(scenegraph_exists, 1)
            self.assertEqual(links_exists, 1)

    def test_migration_backfills_duplicate_group_data(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "legacy_duplicates.db"
            connection = sqlite3.connect(db_path)
            connection.execute(
                """
                CREATE TABLE scan_runs (
                  id INTEGER PRIMARY KEY,
                  root_path TEXT NOT NULL,
                  started_at TEXT,
                  finished_at TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE files (
                  id INTEGER PRIMARY KEY,
                  scan_run_id INTEGER NOT NULL,
                  root_path TEXT NOT NULL,
                  relative_path TEXT NOT NULL,
                  file_name TEXT NOT NULL,
                  extension TEXT NOT NULL,
                  size_bytes INTEGER NOT NULL,
                  modified_at REAL NOT NULL,
                  sha256 TEXT NOT NULL,
                  is_package INTEGER NOT NULL DEFAULT 0,
                  parse_status TEXT NOT NULL DEFAULT 'not_attempted',
                  parse_error TEXT,
                  dbpf_major INTEGER,
                  dbpf_minor INTEGER,
                  index_major INTEGER,
                  index_minor INTEGER,
                  resource_count INTEGER NOT NULL DEFAULT 0,
                  duplicate_group_key TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE duplicate_groups (
                  id INTEGER PRIMARY KEY,
                  sha256 TEXT NOT NULL UNIQUE,
                  file_count INTEGER NOT NULL,
                  total_size_bytes INTEGER NOT NULL
                )
                """
            )
            connection.execute("INSERT INTO scan_runs (id, root_path) VALUES (1, ?)", ("/legacy/root",))
            connection.execute(
                """
                INSERT INTO files (
                  scan_run_id, root_path, relative_path, file_name, extension, size_bytes,
                  modified_at, sha256, duplicate_group_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (1, "/legacy/root", "a.package", "a.package", ".package", 10, 0.0, "samehash", ""),
            )
            connection.execute(
                """
                INSERT INTO files (
                  scan_run_id, root_path, relative_path, file_name, extension, size_bytes,
                  modified_at, sha256, duplicate_group_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (1, "/legacy/root", "b.package", "b.package", ".package", 15, 0.0, "samehash", None),
            )
            connection.execute("PRAGMA user_version = 2")
            connection.commit()
            connection.close()

            initialize_database(db_path)

            connection = connect(db_path)
            file_rows = connection.execute(
                "SELECT duplicate_group_key FROM files ORDER BY relative_path"
            ).fetchall()
            duplicate_row = connection.execute(
                "SELECT sha256, file_count, total_size_bytes FROM duplicate_groups"
            ).fetchone()
            connection.close()

            self.assertEqual([row["duplicate_group_key"] for row in file_rows], ["samehash", "samehash"])
            self.assertEqual(duplicate_row["sha256"], "samehash")
            self.assertEqual(duplicate_row["file_count"], 2)
            self.assertEqual(duplicate_row["total_size_bytes"], 25)

    def test_create_app_uses_env_configured_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "web.db"
            with mock.patch.dict(os.environ, {WEB_DB_ENV_VAR: str(db_path)}):
                app = create_app()
            self.assertEqual(app.state.db_path, db_path.resolve())

    def test_create_app_explicit_database_overrides_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / "env.db"
            explicit_path = Path(tmpdir) / "explicit.db"
            with mock.patch.dict(os.environ, {WEB_DB_ENV_VAR: str(env_path)}):
                app = create_app(explicit_path)
            self.assertEqual(app.state.db_path, explicit_path.resolve())


if __name__ == "__main__":
    unittest.main()
