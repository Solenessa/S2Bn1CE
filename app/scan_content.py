#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import os
import sqlite3
from pathlib import Path

try:
    from app.dbpf_parser import DBPFParseError, parse_dbpf
    from app.resource_parsers import parse_bhav, parse_gzps, parse_objd, parse_ttab
except ModuleNotFoundError:
    from dbpf_parser import DBPFParseError, parse_dbpf
    from resource_parsers import parse_bhav, parse_gzps, parse_objd, parse_ttab


APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
DB_PATH = PROJECT_DIR / "data" / "sims2_cc.db"

PACKAGE_EXTENSIONS = {".package", ".sims2pack"}
PARSEABLE_EXTENSIONS = {".package"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan a Sims 2 content folder into the diagnostics database.")
    parser.add_argument("--root", type=Path, required=True, help="Root folder to scan")
    parser.add_argument("--db", type=Path, default=DB_PATH)
    return parser.parse_args()


def sha256_for_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def scan_files(root: Path) -> list[dict]:
    records: list[dict] = []
    for current_root, _, file_names in os.walk(root):
        current_dir = Path(current_root)
        for file_name in sorted(file_names):
            path = current_dir / file_name
            if not path.is_file():
                continue
            stat = path.stat()
            extension = path.suffix.lower()
            records.append(
                {
                    "relative_path": str(path.relative_to(root)),
                    "file_name": path.name,
                    "extension": extension,
                    "size_bytes": stat.st_size,
                    "modified_at": stat.st_mtime,
                    "sha256": sha256_for_file(path),
                    "is_package": 1 if extension in PACKAGE_EXTENSIONS else 0,
                }
            )
    return records


def purge_root(connection: sqlite3.Connection, root: Path) -> None:
    root_path = str(root)
    connection.execute(
        """
        DELETE FROM pair_reviews
        WHERE left_file_id IN (SELECT id FROM files WHERE root_path = ?)
           OR right_file_id IN (SELECT id FROM files WHERE root_path = ?)
        """,
        (root_path, root_path),
    )
    connection.execute(
        "DELETE FROM objd_objects WHERE file_id IN (SELECT id FROM files WHERE root_path = ?)",
        (root_path,),
    )
    connection.execute(
        "DELETE FROM bhav_functions WHERE file_id IN (SELECT id FROM files WHERE root_path = ?)",
        (root_path,),
    )
    connection.execute(
        "DELETE FROM ttab_tables WHERE file_id IN (SELECT id FROM files WHERE root_path = ?)",
        (root_path,),
    )
    connection.execute(
        "DELETE FROM gzps_entries WHERE file_id IN (SELECT id FROM files WHERE root_path = ?)",
        (root_path,),
    )
    connection.execute(
        "DELETE FROM package_resources WHERE file_id IN (SELECT id FROM files WHERE root_path = ?)",
        (root_path,),
    )
    connection.execute("DELETE FROM files WHERE root_path = ?", (root_path,))


def scan_root(root: Path, db_path: Path = DB_PATH) -> dict[str, int | str]:
    root = root.expanduser().resolve()
    db_path = db_path.expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Root does not exist: {root}")
    if not db_path.exists():
        raise FileNotFoundError(f"Database does not exist: {db_path}. Run bootstrap_db.py first.")

    connection = sqlite3.connect(db_path)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.row_factory = sqlite3.Row

    cursor = connection.execute(
        "INSERT INTO scan_runs (root_path) VALUES (?)",
        (str(root),),
    )
    scan_run_id = int(cursor.lastrowid)

    purge_root(connection, root)
    records = scan_files(root)

    for record in records:
        duplicate_group_key = record["sha256"]
        cursor = connection.execute(
            """
            INSERT INTO files (
              scan_run_id, root_path, relative_path, file_name, extension, size_bytes,
              modified_at, sha256, is_package, parse_status, duplicate_group_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scan_run_id,
                str(root),
                record["relative_path"],
                record["file_name"],
                record["extension"],
                record["size_bytes"],
                record["modified_at"],
                record["sha256"],
                record["is_package"],
                "pending" if record["extension"] in PARSEABLE_EXTENSIONS else "not_attempted",
                duplicate_group_key,
            ),
        )
        file_id = int(cursor.lastrowid)

        if record["extension"] not in PARSEABLE_EXTENSIONS:
            continue

        package_path = root / record["relative_path"]
        try:
            package = parse_dbpf(package_path)
            connection.execute(
                """
                UPDATE files
                SET parse_status = ?, parse_error = NULL, dbpf_major = ?, dbpf_minor = ?,
                    index_major = ?, index_minor = ?, resource_count = ?
                WHERE id = ?
                """,
                (
                    "parsed",
                    package.dbpf_major,
                    package.dbpf_minor,
                    package.index_major,
                    package.index_minor,
                    len(package.resources),
                    file_id,
                ),
            )
            for resource in package.resources:
                resource_cursor = connection.execute(
                    """
                    INSERT INTO package_resources (
                      file_id, type_id, group_id, instance_id, instance_hi,
                      file_offset, file_size, body_sha256, resource_key, type_label, is_dir_record
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        file_id,
                        resource.type_id,
                        resource.group_id,
                        resource.instance_id,
                        resource.instance_hi,
                        resource.file_offset,
                        resource.file_size,
                        resource.body_sha256,
                        resource.resource_key,
                        resource.type_label,
                        1 if resource.is_dir_record else 0,
                    ),
                )
                package_resource_id = int(resource_cursor.lastrowid)

                if resource.type_label == "OBJD" and resource.body is not None:
                    objd = parse_objd(resource.body)
                    connection.execute(
                        """
                        INSERT INTO objd_objects (
                          package_resource_id, file_id, resource_key, object_name, version, guid,
                          original_guid, diagonal_guid, grid_aligned_guid, proxy_guid, job_object_guid,
                          object_model_guid, interaction_table_id, object_type, price, slot_id,
                          catalog_strings_id, function_sort_flags, room_sort_flags, expansion_flag,
                          multi_tile_master_id, multi_tile_sub_index, raw_length
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            package_resource_id,
                            file_id,
                            resource.resource_key,
                            objd.object_name,
                            objd.version,
                            objd.guid,
                            objd.original_guid,
                            objd.diagonal_guid,
                            objd.grid_aligned_guid,
                            objd.proxy_guid,
                            objd.job_object_guid,
                            objd.object_model_guid,
                            objd.interaction_table_id,
                            objd.object_type,
                            objd.price,
                            objd.slot_id,
                            objd.catalog_strings_id,
                            objd.function_sort_flags,
                            objd.room_sort_flags,
                            objd.expansion_flag,
                            objd.multi_tile_master_id,
                            objd.multi_tile_sub_index,
                            objd.raw_length,
                        ),
                    )
                elif resource.type_label == "BHAV" and resource.body is not None:
                    bhav = parse_bhav(resource.body)
                    connection.execute(
                        """
                        INSERT INTO bhav_functions (
                          package_resource_id, file_id, resource_key, function_name, signature,
                          instruction_count, tree_type, arg_count, local_var_count, header_flag,
                          tree_version, instruction_length, first_opcode, last_opcode, raw_length
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            package_resource_id,
                            file_id,
                            resource.resource_key,
                            bhav.function_name,
                            bhav.signature,
                            bhav.instruction_count,
                            bhav.tree_type,
                            bhav.arg_count,
                            bhav.local_var_count,
                            bhav.header_flag,
                            bhav.tree_version,
                            bhav.instruction_length,
                            bhav.first_opcode,
                            bhav.last_opcode,
                            bhav.raw_length,
                        ),
                    )
                elif resource.type_label == "TTAB" and resource.body is not None:
                    ttab = parse_ttab(resource.body)
                    connection.execute(
                        """
                        INSERT INTO ttab_tables (
                          package_resource_id, file_id, resource_key, instance_id, format_code, raw_length
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            package_resource_id,
                            file_id,
                            resource.resource_key,
                            resource.instance_id,
                            ttab.format_code,
                            ttab.raw_length,
                        ),
                    )
                elif resource.type_label == "GZPS" and resource.body is not None:
                    gzps = parse_gzps(resource.body)
                    connection.execute(
                        """
                        INSERT INTO gzps_entries (
                          package_resource_id, file_id, resource_key, name, creator, family,
                          age, gender, species, parts, outfit, flags, product, genetic,
                          type_value, skintone, hairtone, category_bin, raw_length
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            package_resource_id,
                            file_id,
                            resource.resource_key,
                            gzps.name,
                            gzps.creator,
                            gzps.family,
                            gzps.age,
                            gzps.gender,
                            gzps.species,
                            gzps.parts,
                            gzps.outfit,
                            gzps.flags,
                            gzps.product,
                            gzps.genetic,
                            gzps.type_value,
                            gzps.skintone,
                            gzps.hairtone,
                            gzps.category_bin,
                            gzps.raw_length,
                        ),
                    )
        except DBPFParseError as exc:
            connection.execute(
                """
                UPDATE files
                SET parse_status = ?, parse_error = ?, resource_count = 0
                WHERE id = ?
                """,
                ("parse_error", str(exc), file_id),
            )

    connection.execute("DELETE FROM duplicate_groups")
    for row in connection.execute(
        """
        SELECT sha256, COUNT(*) AS file_count, SUM(size_bytes) AS total_size_bytes
        FROM files
        GROUP BY sha256
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

    connection.execute(
        "UPDATE scan_runs SET finished_at = CURRENT_TIMESTAMP WHERE id = ?",
        (scan_run_id,),
    )
    connection.commit()
    scanned_count = connection.execute(
        "SELECT COUNT(*) FROM files WHERE scan_run_id = ?",
        (scan_run_id,),
    ).fetchone()[0]
    duplicate_count = connection.execute("SELECT COUNT(*) FROM duplicate_groups").fetchone()[0]
    parsed_package_count = connection.execute(
        "SELECT COUNT(*) FROM files WHERE scan_run_id = ? AND parse_status = 'parsed'",
        (scan_run_id,),
    ).fetchone()[0]
    resource_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM package_resources pr
        JOIN files f ON f.id = pr.file_id
        WHERE f.scan_run_id = ?
        """,
        (scan_run_id,),
    ).fetchone()[0]
    connection.close()

    return {
        "root": str(root),
        "files_cataloged": scanned_count,
        "duplicate_groups": duplicate_count,
        "parsed_packages": parsed_package_count,
        "indexed_resources": resource_count,
    }


def main() -> None:
    args = parse_args()
    result = scan_root(args.root, args.db)
    print(f"Scan complete for {result['root']}")
    print(f"Files cataloged: {result['files_cataloged']}")
    print(f"Duplicate groups: {result['duplicate_groups']}")
    print(f"Parsed packages: {result['parsed_packages']}")
    print(f"Indexed resources: {result['indexed_resources']}")


if __name__ == "__main__":
    main()
