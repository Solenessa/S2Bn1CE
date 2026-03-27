#!/usr/bin/env python3

from __future__ import annotations

import sqlite3


CURRENT_SCHEMA_VERSION = 3


def _user_version(connection: sqlite3.Connection) -> int:
    return int(connection.execute("PRAGMA user_version").fetchone()[0])


def _set_user_version(connection: sqlite3.Connection, version: int) -> None:
    connection.execute(f"PRAGMA user_version = {version}")


def _migration_1(connection: sqlite3.Connection) -> None:
    # Establish explicit schema versioning for databases that predate migrations.
    connection.execute("SELECT 1")


def _migration_2(connection: sqlite3.Connection) -> None:
    # Bring older databases up to the scan-time dependency metadata layout.
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS scenegraph_names (
          id INTEGER PRIMARY KEY,
          file_id INTEGER NOT NULL,
          source_type_label TEXT NOT NULL,
          resource_key TEXT NOT NULL,
          value TEXT NOT NULL,
          normalized_value TEXT NOT NULL DEFAULT '',
          FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_scenegraph_names_file_id ON scenegraph_names(file_id);
        CREATE INDEX IF NOT EXISTS idx_scenegraph_names_normalized_value ON scenegraph_names(normalized_value);
        CREATE INDEX IF NOT EXISTS idx_scenegraph_names_resource_key ON scenegraph_names(resource_key);

        CREATE TABLE IF NOT EXISTS resource_links (
          id INTEGER PRIMARY KEY,
          file_id INTEGER NOT NULL,
          source_type_label TEXT NOT NULL,
          source_resource_key TEXT NOT NULL,
          target_resource_key TEXT NOT NULL,
          target_type_id INTEGER NOT NULL,
          FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_resource_links_file_id ON resource_links(file_id);
        CREATE INDEX IF NOT EXISTS idx_resource_links_target_resource_key ON resource_links(target_resource_key);
        """
    )


def _migration_3(connection: sqlite3.Connection) -> None:
    # Backfill duplicate grouping for older databases that predate stable
    # duplicate-group persistence or may have stale aggregate rows.
    connection.execute(
        """
        UPDATE files
        SET duplicate_group_key = sha256
        WHERE (duplicate_group_key IS NULL OR duplicate_group_key = '')
          AND sha256 IS NOT NULL
          AND sha256 != ''
        """
    )
    connection.execute("DELETE FROM duplicate_groups")
    for row in connection.execute(
        """
        SELECT duplicate_group_key AS sha256, COUNT(*) AS file_count, SUM(size_bytes) AS total_size_bytes
        FROM files
        WHERE duplicate_group_key IS NOT NULL AND duplicate_group_key != ''
        GROUP BY duplicate_group_key
        HAVING COUNT(*) > 1
        """
    ):
        connection.execute(
            """
            INSERT INTO duplicate_groups (sha256, file_count, total_size_bytes)
            VALUES (?, ?, ?)
            """,
            (row["sha256"], row["file_count"], row["total_size_bytes"]),
        )


MIGRATIONS: dict[int, callable] = {
    1: _migration_1,
    2: _migration_2,
    3: _migration_3,
}


def migrate_database(connection: sqlite3.Connection) -> int:
    version = _user_version(connection)
    while version < CURRENT_SCHEMA_VERSION:
        target = version + 1
        migration = MIGRATIONS[target]
        migration(connection)
        _set_user_version(connection, target)
        version = target
    return version
