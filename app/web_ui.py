#!/usr/bin/env python3

from __future__ import annotations

import html
import re
import sqlite3
from pathlib import PurePosixPath
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse

try:
    from app.dbpf_parser import parse_dbpf
    from app.resource_parsers import parse_3idr, parse_txmt
except ModuleNotFoundError:
    from dbpf_parser import parse_dbpf
    from resource_parsers import parse_3idr, parse_txmt


APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
DB_PATH = PROJECT_DIR / "data" / "sims2_cc.db"
app = FastAPI(title="Sims 2 CC Diagnostics")
HIGH_RISK_RESOURCE_TYPES = {"BHAV", "BCON", "GLOB", "OBJD", "TTAB", "TTAs", "TPRP"}
CAS_MESH_TYPES = {"GMDC", "GMND", "CRES", "SHPE"}
CAS_RECOLOR_TYPES = {"GZPS", "TXMT", "TXTR", "3IDR"}
HACK_TYPES = {"BHAV", "BCON", "GLOB", "OBJD", "TTAB", "TTAs", "TPRP", "TRCN"}
SCENEGRAPH_TYPES = {"GMDC", "GMND", "CRES", "SHPE", "TXMT", "TXTR", "3IDR", "GZPS"}
NAME_PATTERN = re.compile(r"[A-Za-z0-9_.!#~:-]{4,}")


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.execute("PRAGMA foreign_keys = ON")
    connection.row_factory = sqlite3.Row
    return connection


def pair_key(left_file_id: int, right_file_id: int) -> tuple[int, int]:
    if left_file_id <= right_file_id:
        return left_file_id, right_file_id
    return right_file_id, left_file_id


def fetch_review_map(connection: sqlite3.Connection) -> dict[tuple[int, int], sqlite3.Row]:
    rows = connection.execute(
        "SELECT left_file_id, right_file_id, status, note, updated_at FROM pair_reviews"
    ).fetchall()
    return {(row["left_file_id"], row["right_file_id"]): row for row in rows}


def save_review(left_file_id: int, right_file_id: int, status: str) -> None:
    pair_left, pair_right = pair_key(left_file_id, right_file_id)
    connection = get_connection()
    connection.execute(
        """
        INSERT INTO pair_reviews (left_file_id, right_file_id, status, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(left_file_id, right_file_id)
        DO UPDATE SET status = excluded.status, updated_at = CURRENT_TIMESTAMP
        """,
        (pair_left, pair_right, status),
    )
    connection.commit()
    connection.close()


def clear_review(left_file_id: int, right_file_id: int) -> None:
    pair_left, pair_right = pair_key(left_file_id, right_file_id)
    connection = get_connection()
    connection.execute(
        "DELETE FROM pair_reviews WHERE left_file_id = ? AND right_file_id = ?",
        (pair_left, pair_right),
    )
    connection.commit()
    connection.close()


def page(title: str, body: str) -> HTMLResponse:
    return HTMLResponse(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f5f1ea;
      --panel: #fffaf3;
      --line: #d8cebf;
      --ink: #1f1a17;
      --muted: #665f57;
      --accent: #8b4f2b;
      --high: #8a3324;
      --low: #2f7254;
      --shadow: 0 18px 38px rgba(61, 42, 22, 0.12);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Palatino Linotype", serif;
      color: var(--ink);
      background: linear-gradient(180deg, #faf6ef 0%, var(--bg) 100%);
    }}
    a {{ color: var(--accent); text-decoration: none; }}
    .shell {{ max-width: 1360px; margin: 0 auto; padding: 24px; }}
    .hero, .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      box-shadow: var(--shadow);
    }}
    .hero {{ padding: 28px; }}
    .hero h1 {{ margin: 0 0 8px; font-size: clamp(2rem, 4vw, 3.6rem); letter-spacing: -0.04em; }}
    .hero p {{ margin: 0; color: var(--muted); max-width: 62rem; }}
    .hero-actions, .meta, .actions, .chips {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .hero-actions {{ margin-top: 12px; }}
    .layout {{ display: grid; grid-template-columns: 320px minmax(0, 1fr); gap: 20px; margin-top: 20px; }}
    .panel-inner {{ padding: 18px; }}
    .stat-grid {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px; }}
    .stat, .card {{
      padding: 14px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: #fffdf8;
    }}
    .stat strong {{ display: block; font-size: 1.4rem; margin-top: 6px; }}
    .mod-list {{ display: grid; gap: 12px; }}
    .subtle {{ color: var(--muted); font-size: 0.92rem; }}
    .chip {{
      display: inline-flex;
      padding: 5px 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
      font-size: 0.84rem;
    }}
    .chip.high {{ color: var(--high); }}
    .chip.low {{ color: var(--low); }}
    .summary {{ color: var(--muted); line-height: 1.5; }}
    .actions a {{ padding: 8px 12px; border: 1px solid var(--line); border-radius: 999px; background: #fff; }}
    .grid-2 {{ display: grid; grid-template-columns: repeat(2, 1fr); gap: 20px; margin-top: 20px; }}
    @media (max-width: 960px) {{
      .layout, .grid-2 {{ grid-template-columns: 1fr; }}
      .shell {{ padding: 14px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">{body}</div>
</body>
</html>"""
    )


def fetch_stats(connection: sqlite3.Connection) -> sqlite3.Row:
    return connection.execute(
        """
        SELECT
          COUNT(*) AS file_count,
          SUM(CASE WHEN is_package = 1 THEN 1 ELSE 0 END) AS package_count,
          SUM(CASE WHEN parse_status = 'parsed' THEN 1 ELSE 0 END) AS parsed_package_count,
          COUNT(DISTINCT root_path) AS roots_scanned,
          (SELECT COUNT(*) FROM duplicate_groups) AS duplicate_group_count,
          (SELECT COUNT(*) FROM pair_reviews WHERE status = 'confirmed') AS confirmed_pairs,
          (SELECT COUNT(*) FROM package_resources) AS resource_count,
          (
            SELECT COUNT(*)
            FROM (
              SELECT resource_key
              FROM package_resources
              WHERE is_dir_record = 0
              GROUP BY resource_key
              HAVING COUNT(DISTINCT file_id) > 1
            )
          ) AS resource_conflict_group_count,
          (SELECT COUNT(*) FROM objd_objects) AS objd_count,
          (SELECT COUNT(*) FROM bhav_functions) AS bhav_count,
          (SELECT COUNT(*) FROM ttab_tables) AS ttab_count,
          (
            SELECT COUNT(*)
            FROM (
              SELECT guid
              FROM objd_objects
              WHERE guid IS NOT NULL AND guid != 0
              GROUP BY guid
              HAVING COUNT(DISTINCT file_id) > 1
            )
          ) AS objd_guid_conflict_count
          ,
          (SELECT COUNT(*) FROM gzps_entries) AS gzps_count,
          (SELECT COUNT(*) FROM crash_reports WHERE log_type = 'crash') AS crash_count,
          (SELECT COUNT(*) FROM crash_reports WHERE log_type = 'config') AS config_count
        FROM files
        """
    ).fetchone()


def fetch_recent_files(connection: sqlite3.Connection, query: str = "", limit: int = 80) -> list[sqlite3.Row]:
    params: list[object] = []
    where_sql = ""
    if query.strip():
        like_value = f"%{query.strip().lower()}%"
        where_sql = """
        WHERE
          lower(relative_path) LIKE ?
          OR lower(file_name) LIKE ?
          OR lower(extension) LIKE ?
        """
        params.extend([like_value, like_value, like_value])
    params.append(limit)
    return connection.execute(
        f"""
        SELECT id, relative_path, file_name, extension, size_bytes, is_package, sha256, parse_status, resource_count
        FROM files
        {where_sql}
        ORDER BY relative_path
        LIMIT ?
        """,
        params,
    ).fetchall()


def fetch_duplicate_groups(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, sha256, file_count, total_size_bytes
        FROM duplicate_groups
        ORDER BY file_count DESC, total_size_bytes DESC
        LIMIT 40
        """
    ).fetchall()


def fetch_duplicate_files(connection: sqlite3.Connection, sha256: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, relative_path, file_name, size_bytes
        FROM files
        WHERE sha256 = ?
        ORDER BY relative_path
        """,
        (sha256,),
    ).fetchall()


def fetch_resource_conflict_groups(connection: sqlite3.Connection, limit: int = 40) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          pr.resource_key,
          COALESCE(MAX(pr.type_label), 'Unknown') AS type_label,
          COUNT(DISTINCT pr.file_id) AS file_count,
          COUNT(DISTINCT COALESCE(pr.body_sha256, '')) AS variant_count
        FROM package_resources pr
        WHERE pr.is_dir_record = 0
        GROUP BY pr.resource_key
        HAVING COUNT(DISTINCT pr.file_id) > 1
        ORDER BY variant_count DESC, file_count DESC, type_label, pr.resource_key
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def fetch_resource_conflict_files(connection: sqlite3.Connection, resource_key: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          f.id,
          f.relative_path,
          f.file_name,
          pr.type_label,
          pr.file_offset,
          pr.file_size,
          pr.body_sha256
        FROM package_resources pr
        JOIN files f ON f.id = pr.file_id
        WHERE pr.resource_key = ?
        ORDER BY f.relative_path
        """,
        (resource_key,),
    ).fetchall()


def fetch_resource_strings(connection: sqlite3.Connection, file_id: int, type_labels: set[str] | None = None) -> list[sqlite3.Row]:
    params: list[object] = [file_id]
    type_filter = ""
    if type_labels:
        placeholders = ", ".join("?" for _ in sorted(type_labels))
        type_filter = f" AND pr.type_label IN ({placeholders})"
        params.extend(sorted(type_labels))
    return connection.execute(
        f"""
        SELECT pr.type_label, pr.resource_key
        FROM package_resources pr
        WHERE pr.file_id = ? AND pr.is_dir_record = 0 {type_filter}
        ORDER BY pr.type_label, pr.resource_key
        """,
        params,
    ).fetchall()


def build_package_cache(connection: sqlite3.Connection) -> dict[int, dict]:
    rows = connection.execute(
        """
        SELECT id, root_path, relative_path, file_name
        FROM files
        WHERE parse_status = 'parsed' AND is_package = 1
        ORDER BY relative_path
        """
    ).fetchall()
    cache: dict[int, dict] = {}
    for row in rows:
        package_path = Path(row["root_path"]) / row["relative_path"]
        package = parse_dbpf(package_path)
        cache[row["id"]] = {
            "file_id": row["id"],
            "relative_path": row["relative_path"],
            "file_name": row["file_name"],
            "package": package,
            "resources_by_key": {resource.resource_key: resource for resource in package.resources},
        }
    return cache


def build_resource_owner_map(connection: sqlite3.Connection) -> dict[str, list[dict]]:
    rows = connection.execute(
        """
        SELECT pr.resource_key, pr.type_label, f.id AS file_id, f.relative_path, f.file_name
        FROM package_resources pr
        JOIN files f ON f.id = pr.file_id
        WHERE pr.is_dir_record = 0
        ORDER BY pr.resource_key, f.relative_path
        """
    ).fetchall()
    owners: dict[str, list[dict]] = {}
    for row in rows:
        owners.setdefault(row["resource_key"], []).append(
            {
                "file_id": row["file_id"],
                "relative_path": row["relative_path"],
                "file_name": row["file_name"],
                "type_label": row["type_label"],
            }
        )
    return owners


def fetch_resource_type_breakdown(connection: sqlite3.Connection, limit: int = 16) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT COALESCE(type_label, 'Unknown') AS type_label, COUNT(*) AS resource_count
        FROM package_resources
        WHERE is_dir_record = 0
        GROUP BY type_label
        ORDER BY resource_count DESC, type_label
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def fetch_objd_guid_conflicts(connection: sqlite3.Connection, limit: int = 40) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT guid, COUNT(DISTINCT file_id) AS file_count, COUNT(*) AS object_count
        FROM objd_objects
        WHERE guid IS NOT NULL AND guid != 0
        GROUP BY guid
        HAVING COUNT(DISTINCT file_id) > 1
        ORDER BY file_count DESC, object_count DESC, guid
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def fetch_objd_conflict_files(connection: sqlite3.Connection, guid: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          oo.file_id,
          oo.object_name,
          oo.guid,
          oo.original_guid,
          oo.object_type,
          oo.price,
          oo.expansion_flag,
          f.relative_path,
          f.file_name
        FROM objd_objects oo
        JOIN files f ON f.id = oo.file_id
        WHERE oo.guid = ?
        ORDER BY f.relative_path, oo.object_name
        """,
        (guid,),
    ).fetchall()


def fetch_bhav_heavy_files(connection: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          f.id,
          f.relative_path,
          COUNT(*) AS bhav_count,
          COALESCE(SUM(COALESCE(bf.instruction_count, 0)), 0) AS total_instructions,
          MAX(COALESCE(bf.instruction_count, 0)) AS max_instruction_count
        FROM bhav_functions bf
        JOIN files f ON f.id = bf.file_id
        GROUP BY f.id
        ORDER BY bhav_count DESC, total_instructions DESC, f.relative_path
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def fetch_ttab_heavy_files(connection: sqlite3.Connection, limit: int = 20) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          f.id,
          f.relative_path,
          COUNT(*) AS ttab_count,
          COUNT(DISTINCT oo.id) AS linked_objd_count
        FROM ttab_tables tt
        JOIN files f ON f.id = tt.file_id
        LEFT JOIN objd_objects oo ON oo.interaction_table_id = tt.instance_id
        GROUP BY f.id
        ORDER BY ttab_count DESC, linked_objd_count DESC, f.relative_path
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def fetch_ttab_links(connection: sqlite3.Connection, limit: int = 30) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          tt.file_id,
          f.relative_path,
          tt.instance_id,
          tt.format_code,
          COUNT(DISTINCT oo.id) AS linked_objd_count,
          GROUP_CONCAT(DISTINCT oo.object_name) AS object_names
        FROM ttab_tables tt
        JOIN files f ON f.id = tt.file_id
        LEFT JOIN objd_objects oo ON oo.interaction_table_id = tt.instance_id
        GROUP BY tt.id
        ORDER BY linked_objd_count DESC, f.relative_path, tt.instance_id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def classify_package(resource_types: set[str]) -> tuple[str, str]:
    if resource_types & HACK_TYPES:
        if "OBJD" in resource_types or "TTAB" in resource_types:
            return "Object or Hack Package", "Contains object-definition or behavior resources and should be treated as gameplay-affecting content."
        return "Behavior Mod", "Contains behavior-level resources such as BHAV, BCON, or GLOB."
    has_mesh = bool(resource_types & CAS_MESH_TYPES)
    has_recolor = bool(resource_types & CAS_RECOLOR_TYPES)
    if has_mesh and has_recolor:
        return "CAS Hybrid Set", "Contains both scenegraph mesh resources and recolor/property resources."
    if has_mesh:
        return "Mesh Package", "Contains scenegraph or mesh resources and is likely a required mesh dependency."
    if has_recolor:
        return "Recolor or Property Set", "Contains recolor/property resources such as GZPS, TXMT, TXTR, or 3IDR."
    if resource_types == {"STR#"} or resource_types == {"CTSS"}:
        return "Text Support Package", "Mostly or entirely string resources."
    return "Unclassified Package", "Needs more specific parsing to classify confidently."


def extract_printable_strings(blob: bytes) -> list[str]:
    strings = []
    seen: set[str] = set()
    for match in NAME_PATTERN.finditer(blob.decode("latin-1", errors="ignore")):
        value = match.group(0)
        if value not in seen:
            seen.add(value)
            strings.append(value)
    return strings


def normalize_scenegraph_name(value: str) -> str:
    normalized = value.lower()
    normalized = normalized.replace("##", "")
    normalized = re.sub(r"^0x[0-9a-f]+!", "", normalized)
    normalized = normalized.replace("mesh.", "")
    normalized = normalized.replace("!body~", "body_")
    normalized = normalized.replace("!body_", "body_")
    normalized = normalized.replace("~", "_")
    normalized = normalized.replace("-", "_")
    normalized = normalized.replace(".", "_")
    normalized = re.sub(r"_(txmt|txt|gmdc|gmnd|shpe|cres)$", "", normalized)
    normalized = re.sub(r"_untagged\d+$", "", normalized)
    normalized = re.sub(r"_tslocator$", "", normalized)
    normalized = re.sub(r"_lod\d+$", "", normalized)
    normalized = re.sub(r"__+", "_", normalized).strip("_")
    return normalized


def important_scenegraph_string(value: str) -> bool:
    lowered = value.lower()
    if len(value) < 8:
        return False
    return any(token in lowered for token in ["mesh.", "body_", "body~", "_txmt", "_txt", "_gmdc", "_gmnd", "_shpe", "_cres", "stdmat", "locator", "pirate", "hoodie", "dress", "vest"])


def extract_name_tokens(value: str) -> set[str]:
    lowered = value.lower()
    parts = re.split(r"[^a-z0-9]+", lowered)
    merged = []
    for part in parts:
        if len(part) >= 4:
            merged.append(part)
        camel_parts = re.findall(r"[a-z]+|\d+", part)
        for token in camel_parts:
            if len(token) >= 4:
                merged.append(token)
    return set(merged)


def build_scenegraph_signatures(connection: sqlite3.Connection, package_cache: dict[int, dict] | None = None) -> dict[int, dict]:
    if package_cache is None:
        package_cache = build_package_cache(connection)
    signatures: dict[int, dict] = {}
    file_rows = connection.execute(
        """
        SELECT id, relative_path, file_name
        FROM files
        WHERE parse_status = 'parsed' AND is_package = 1
        ORDER BY relative_path
        """
    ).fetchall()
    for file_row in file_rows:
        file_id = file_row["id"]
        resources = connection.execute(
            """
            SELECT pr.type_label, pr.resource_key, pr.file_offset, pr.file_size, f.root_path, f.relative_path
            FROM package_resources pr
            JOIN files f ON f.id = pr.file_id
            WHERE pr.file_id = ? AND pr.is_dir_record = 0 AND pr.type_label IN ('GMDC', 'GMND', 'CRES', 'SHPE', 'TXMT', 'TXTR', '3IDR', 'GZPS')
            ORDER BY pr.type_label, pr.resource_key
            """,
            (file_id,),
        ).fetchall()
        raw_strings: list[str] = []
        normalized_names: set[str] = set()
        tokens = extract_name_tokens(file_row["file_name"]) | extract_name_tokens(file_row["relative_path"])
        package_entry = package_cache.get(file_id)
        for resource in resources:
            resource_blob = None
            if package_entry:
                candidate = package_entry["resources_by_key"].get(resource["resource_key"])
                if candidate and candidate.type_label == resource["type_label"]:
                    resource_blob = candidate.body
            if not resource_blob:
                continue
            for value in extract_printable_strings(resource_blob):
                if important_scenegraph_string(value):
                    raw_strings.append(value)
                    normalized = normalize_scenegraph_name(value)
                    if normalized:
                        normalized_names.add(normalized)
                        tokens |= extract_name_tokens(normalized)
        signatures[file_id] = {
            "file_id": file_id,
            "relative_path": file_row["relative_path"],
            "file_name": file_row["file_name"],
            "raw_strings": sorted(set(raw_strings))[:24],
            "normalized_names": sorted(normalized_names),
            "tokens": sorted(tokens),
        }
    return signatures


def fetch_package_profiles(connection: sqlite3.Connection, limit: int = 120) -> list[dict]:
    rows = connection.execute(
        """
        SELECT
          f.id,
          f.relative_path,
          f.file_name,
          f.sha256,
          f.parse_status,
          f.resource_count,
          GROUP_CONCAT(DISTINCT pr.type_label) AS type_labels
        FROM files f
        LEFT JOIN package_resources pr ON pr.file_id = f.id AND pr.is_dir_record = 0
        WHERE f.is_package = 1
        GROUP BY f.id
        ORDER BY f.relative_path
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    profiles = []
    for row in rows:
        types = set(filter(None, (row["type_labels"] or "").split(",")))
        category, explanation = classify_package(types)
        profiles.append(
            {
                "id": row["id"],
                "relative_path": row["relative_path"],
                "file_name": row["file_name"],
                "sha256": row["sha256"],
                "parse_status": row["parse_status"],
                "resource_count": row["resource_count"],
                "resource_types": sorted(types),
                "category": category,
                "explanation": explanation,
            }
        )
    return profiles


def fetch_category_breakdown(connection: sqlite3.Connection) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for profile in fetch_package_profiles(connection, limit=5000):
        counts[profile["category"]] = counts.get(profile["category"], 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def fetch_gzps_summary(connection: sqlite3.Connection, file_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT name, creator, family, age, gender, species, parts, outfit, type_value
        FROM gzps_entries
        WHERE file_id = ?
        ORDER BY name, creator
        """,
        (file_id,),
    ).fetchall()


def fetch_package_metadata_map(connection: sqlite3.Connection) -> dict[int, list[sqlite3.Row]]:
    rows = connection.execute(
        """
        SELECT file_id, name, creator, family, age, gender, species, parts, outfit, type_value
        FROM gzps_entries
        ORDER BY file_id, name
        """
    ).fetchall()
    result: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        result.setdefault(row["file_id"], []).append(row)
    return result


def fetch_creator_clusters(connection: sqlite3.Connection) -> list[dict]:
    rows = connection.execute(
        """
        SELECT
          creator,
          COUNT(DISTINCT file_id) AS package_count,
          COUNT(DISTINCT name) AS item_count
        FROM gzps_entries
        WHERE creator IS NOT NULL AND creator != ''
        GROUP BY creator
        HAVING COUNT(DISTINCT file_id) > 1
        ORDER BY package_count DESC, creator
        """
    ).fetchall()
    clusters = []
    for row in rows:
        packages = connection.execute(
            """
            SELECT DISTINCT f.id, f.relative_path, gz.name
            FROM gzps_entries gz
            JOIN files f ON f.id = gz.file_id
            WHERE gz.creator = ?
            ORDER BY f.relative_path
            """,
            (row["creator"],),
        ).fetchall()
        clusters.append(
            {
                "creator": row["creator"],
                "package_count": row["package_count"],
                "item_count": row["item_count"],
                "packages": packages,
            }
        )
    return clusters


def fetch_material_override_groups(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          pr.type_label,
          pr.resource_key,
          COUNT(DISTINCT pr.file_id) AS file_count,
          COUNT(DISTINCT COALESCE(pr.body_sha256, '')) AS variant_count
        FROM package_resources pr
        WHERE pr.is_dir_record = 0
          AND pr.type_label IN ('TXMT', 'TXTR')
        GROUP BY pr.resource_key, pr.type_label
        HAVING COUNT(DISTINCT pr.file_id) > 1 AND COUNT(DISTINCT COALESCE(pr.body_sha256, '')) > 1
        ORDER BY variant_count DESC, file_count DESC, pr.type_label, pr.resource_key
        """
    ).fetchall()


def fetch_partial_gzps_rows(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT f.id, f.relative_path, gz.name, gz.creator, gz.family, gz.type_value
        FROM gzps_entries gz
        JOIN files f ON f.id = gz.file_id
        WHERE gz.name = ''
           OR gz.creator = ''
           OR gz.family = ''
        ORDER BY f.relative_path
        """
    ).fetchall()


def fetch_file_row(connection: sqlite3.Connection, file_id: int) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id, root_path, relative_path, file_name, extension, size_bytes, sha256, parse_status, resource_count
        FROM files
        WHERE id = ?
        """,
        (file_id,),
    ).fetchone()


def fetch_crash_reports(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
          id, source_path, file_name, log_type, occurred_at_text, app_name, exception_code,
          exception_module, fault_address, crash_category, summary, graphics_device,
          graphics_vendor, driver_version, texture_memory_mb, os_version, memory_hint,
          imported_at
        FROM crash_reports
        ORDER BY imported_at DESC, file_name
        """
    ).fetchall()


def correlate_crash_report(connection: sqlite3.Connection, crash_row: sqlite3.Row) -> list[str]:
    notes: list[str] = []
    category = crash_row["crash_category"]
    if category == "graphics":
        txtr_count = connection.execute("SELECT COUNT(*) FROM package_resources WHERE type_label = 'TXTR'").fetchone()[0]
        notes.append(f"Current scanned content includes {txtr_count} texture resources, so texture-heavy CAS content could amplify graphics instability.")
        if crash_row["texture_memory_mb"] is not None and crash_row["texture_memory_mb"] < 256:
            notes.append("Parsed texture memory is low for a modded Sims 2 setup. Graphics rules or texture-memory tuning may matter more than package conflicts.")
    elif category == "memory":
        notes.append("This looks like an access-violation style crash. Treat both system instability and broken package interactions as live possibilities.")
        mesh_dupes = len(fetch_duplicate_mesh_candidates(connection))
        if mesh_dupes:
            notes.append(f"The current scan also shows {mesh_dupes} duplicate mesh-package groups, which can complicate isolation.")
    elif category == "object-hack":
        hack_packages = connection.execute(
            """
            SELECT COUNT(*)
            FROM files f
            JOIN package_resources pr ON pr.file_id = f.id
            WHERE pr.type_label IN ('BHAV', 'OBJD', 'TTAB', 'BCON', 'GLOB')
            """
        ).fetchone()[0]
        notes.append(f"The current content scan exposes {hack_packages} high-impact gameplay resources. Object/hack conflicts deserve priority.")
    elif category == "startup":
        config_count = connection.execute("SELECT COUNT(*) FROM crash_reports WHERE log_type = 'config'").fetchone()[0]
        notes.append("Startup or initialization crashes often correlate more strongly with config, graphics rules, or missing dependencies than with random CAS recolors.")
        if config_count:
            notes.append(f"There are {config_count} imported config logs available for cross-checking GPU and texture-memory settings.")
    filenames = [row["file_name"] for row in fetch_package_profiles(connection, limit=5000)]
    raw_text = connection.execute("SELECT raw_text FROM crash_reports WHERE id = ?", (crash_row["id"],)).fetchone()[0].lower()
    mentioned = [name for name in filenames if len(name) > 8 and name.lower() in raw_text][:5]
    if mentioned:
        notes.append(f"The log text mentions package-like filenames that also exist in the scan: {', '.join(mentioned)}.")
    if not notes:
        notes.append("No strong package-side correlation was inferred from the current scan. Treat this log as primarily system-side until more evidence appears.")
    return notes


def fetch_package_detail(connection: sqlite3.Connection, file_id: int) -> dict | None:
    file_row = fetch_file_row(connection, file_id)
    if not file_row:
        return None
    profiles = {profile["id"]: profile for profile in fetch_package_profiles(connection, limit=5000)}
    profile = profiles.get(file_id)
    resource_rows = connection.execute(
        """
        SELECT type_label, resource_key, file_size, body_sha256
        FROM package_resources
        WHERE file_id = ? AND is_dir_record = 0
        ORDER BY type_label, resource_key
        """,
        (file_id,),
    ).fetchall()
    type_counts: dict[str, int] = {}
    for row in resource_rows:
        type_counts[row["type_label"] or "Unknown"] = type_counts.get(row["type_label"] or "Unknown", 0) + 1
    return {
        "file": file_row,
        "profile": profile,
        "gzps": fetch_gzps_summary(connection, file_id),
        "resources": resource_rows,
        "type_counts": sorted(type_counts.items(), key=lambda item: (-item[1], item[0])),
    }


def build_folder_dependency_groups(connection: sqlite3.Connection) -> list[dict]:
    groups: dict[str, dict] = {}
    for profile in fetch_package_profiles(connection, limit=5000):
        folder = str(PurePosixPath(profile["relative_path"]).parent)
        bucket = groups.setdefault(
            folder,
            {
                "folder": folder,
                "mesh_packages": [],
                "recolor_packages": [],
                "hybrid_packages": [],
                "other_packages": [],
            },
        )
        category = profile["category"]
        if category == "Mesh Package":
            bucket["mesh_packages"].append(profile)
        elif category == "Recolor or Property Set":
            bucket["recolor_packages"].append(profile)
        elif category == "CAS Hybrid Set":
            bucket["hybrid_packages"].append(profile)
        else:
            bucket["other_packages"].append(profile)

    results = []
    for group in groups.values():
        results.append(
            {
                **group,
                "likely_bundle": bool(group["mesh_packages"] and group["recolor_packages"]),
                "package_count": sum(len(group[key]) for key in ["mesh_packages", "recolor_packages", "hybrid_packages", "other_packages"]),
                "bundle_score": (len(group["mesh_packages"]) * 3) + (len(group["hybrid_packages"]) * 2) + len(group["recolor_packages"]),
                "has_dependency_anchor": bool(group["mesh_packages"] or group["hybrid_packages"]),
            }
        )
    return results


def fetch_dependency_groups(connection: sqlite3.Connection) -> list[dict]:
    results = []
    for group in build_folder_dependency_groups(connection):
        if not group["likely_bundle"] and not group["hybrid_packages"]:
            continue
        results.append(group)
    results.sort(key=lambda item: (-item["likely_bundle"], -item["package_count"], item["folder"]))
    return results


def score_dependency_pair(recolor_signature: dict, mesh_signature: dict) -> tuple[int, list[str]]:
    recolor_names = set(recolor_signature["normalized_names"])
    mesh_names = set(mesh_signature["normalized_names"])
    overlap = sorted(recolor_names & mesh_names)
    recolor_tokens = set(recolor_signature.get("tokens", []))
    mesh_tokens = set(mesh_signature.get("tokens", []))
    token_overlap = sorted((recolor_tokens & mesh_tokens) - {"mesh", "package", "body", "fakepeeps7", "coloursmesh"})
    reasons: list[str] = []
    score = 0
    if overlap:
        score += min(12, len(overlap) * 3)
        reasons.append(f"{len(overlap)} shared scenegraph names")
    if token_overlap:
        score += min(6, len(token_overlap) * 2)
        reasons.append(f"{len(token_overlap)} shared name tokens")
    recolor_folder = str(PurePosixPath(recolor_signature["relative_path"]).parent)
    mesh_folder = str(PurePosixPath(mesh_signature["relative_path"]).parent)
    if recolor_folder == mesh_folder:
        score += 5
        reasons.append("same folder")
    if recolor_signature["file_name"].startswith("MESH_") == mesh_signature["file_name"].startswith("MESH_"):
        pass
    if "body" in " ".join(overlap):
        score += 2
    return score, reasons


def fetch_dependency_candidates(connection: sqlite3.Connection) -> tuple[dict[int, list[dict]], list[dict], dict[int, dict]]:
    package_cache = build_package_cache(connection)
    signatures = build_scenegraph_signatures(connection, package_cache=package_cache)
    resource_owner_map = build_resource_owner_map(connection)
    profiles = {profile["id"]: profile for profile in fetch_package_profiles(connection, limit=5000)}
    mesh_profiles = [profile for profile in profiles.values() if profile["category"] in {"Mesh Package", "CAS Hybrid Set"}]
    recolor_profiles = [profile for profile in profiles.values() if profile["category"] in {"Recolor or Property Set", "CAS Hybrid Set"}]

    candidates_by_recolor: dict[int, list[dict]] = {}
    unresolved: list[dict] = []
    for recolor in recolor_profiles:
        recolor_sig = signatures.get(recolor["id"], {"normalized_names": [], "raw_strings": [], "relative_path": recolor["relative_path"], "file_name": recolor["file_name"]})
        scored_map: dict[int, dict] = {}
        package_entry = package_cache.get(recolor["id"])
        if package_entry:
            for resource in package_entry["package"].resources:
                if resource.type_label != "3IDR" or not resource.body:
                    continue
                parsed_3idr = parse_3idr(resource.body)
                for ref in parsed_3idr.references:
                    for owner in resource_owner_map.get(ref.resource_key, []):
                        if owner["file_id"] == recolor["id"]:
                            continue
                        owner_profile = profiles.get(owner["file_id"])
                        if not owner_profile or owner_profile["category"] not in {"Mesh Package", "CAS Hybrid Set"}:
                            continue
                        candidate = scored_map.setdefault(
                            owner["file_id"],
                            {
                                "mesh_id": owner["file_id"],
                                "mesh_path": owner["relative_path"],
                                "mesh_name": owner["file_name"],
                                "score": 0,
                                "reasons": [],
                                "shared_names": [],
                                "explicit_refs": [],
                                "material_names": [],
                            },
                        )
                        candidate["score"] += 18
                        candidate["reasons"].append(f"explicit 3IDR ref to {ref.type_id:08X}")
                        candidate["explicit_refs"].append(ref.resource_key)
                txmt_names = []
                for sibling in package_entry["package"].resources:
                    if sibling.type_label == "TXMT" and sibling.body:
                        parsed_txmt = parse_txmt(sibling.body)
                        if parsed_txmt.resource_name:
                            txmt_names.append(parsed_txmt.resource_name)
                if txmt_names:
                    for candidate in scored_map.values():
                        candidate["material_names"] = sorted(set(txmt_names))[:4]

        for mesh in mesh_profiles:
            if mesh["id"] == recolor["id"]:
                continue
            mesh_sig = signatures.get(mesh["id"], {"normalized_names": [], "raw_strings": [], "relative_path": mesh["relative_path"], "file_name": mesh["file_name"]})
            score, reasons = score_dependency_pair(recolor_sig, mesh_sig)
            if score <= 0:
                continue
            candidate = scored_map.setdefault(
                mesh["id"],
                {
                    "mesh_id": mesh["id"],
                    "mesh_path": mesh["relative_path"],
                    "mesh_name": mesh["file_name"],
                    "score": 0,
                    "reasons": [],
                    "shared_names": [],
                    "explicit_refs": [],
                    "material_names": [],
                },
            )
            candidate["score"] += score
            candidate["reasons"].extend(reasons)
            candidate["shared_names"] = sorted(set(candidate["shared_names"]) | (set(recolor_sig["normalized_names"]) & set(mesh_sig["normalized_names"])))[:8]
        scored = list(scored_map.values())
        for candidate in scored:
            candidate["reasons"] = sorted(set(candidate["reasons"]))
            candidate["explicit_refs"] = sorted(set(candidate["explicit_refs"]))[:6]
        scored.sort(key=lambda item: (-item["score"], item["mesh_path"]))
        candidates_by_recolor[recolor["id"]] = scored[:5]
        if not scored or scored[0]["score"] < 7:
            unresolved.append(recolor)
    return candidates_by_recolor, unresolved, signatures


def fetch_orphan_recolor_groups(connection: sqlite3.Connection) -> list[dict]:
    results = []
    for group in build_folder_dependency_groups(connection):
        if not group["recolor_packages"]:
            continue
        if group["has_dependency_anchor"]:
            continue
        results.append(group)
    results.sort(key=lambda item: (-len(item["recolor_packages"]), -item["package_count"], item["folder"]))
    return results


def fetch_duplicate_mesh_candidates(connection: sqlite3.Connection) -> list[dict]:
    groups: dict[str, dict] = {}
    for profile in fetch_package_profiles(connection, limit=5000):
        if profile["category"] not in {"Mesh Package", "CAS Hybrid Set"}:
            continue
        bucket = groups.setdefault(
            profile["file_name"],
            {
                "file_name": profile["file_name"],
                "items": [],
                "sha256_values": set(),
            },
        )
        bucket["items"].append(profile)
        bucket["sha256_values"].add(profile["sha256"])

    results = []
    for group in groups.values():
        if len(group["items"]) < 2:
            continue
        exact_duplicate = len(group["sha256_values"]) == 1
        results.append(
            {
                "file_name": group["file_name"],
                "items": sorted(group["items"], key=lambda item: item["relative_path"]),
                "copy_count": len(group["items"]),
                "variant_count": len(group["sha256_values"]),
                "exact_duplicate": exact_duplicate,
                "severity": "Low" if exact_duplicate else "Medium",
                "explanation": (
                    "Exact duplicate mesh package distributed in multiple folders. This is usually harmless but redundant, and it can hide which copy is actually required."
                    if exact_duplicate
                    else "Mesh packages share the same filename but not the same file hash. Treat this as a likely variant collision until reviewed."
                ),
            }
        )
    results.sort(key=lambda item: (-item["copy_count"], item["variant_count"], item["file_name"]))
    return results


def classify_cas_asset_group(type_label: str, variant_count: int) -> tuple[str, str, str]:
    if variant_count <= 1:
        if type_label in CAS_MESH_TYPES:
            return "Low", "Duplicate Mesh Asset", "Same scenegraph resource and same payload across multiple packages. This usually points at redistributed mesh content rather than a true conflict."
        if type_label in {"TXMT", "TXTR"}:
            return "Low", "Shared Texture Asset", "Same material or texture payload across packages. This is often intentional reuse or duplicate distribution."
        return "Low", "Shared CAS Asset", "Same CAS-facing resource and same payload across packages."
    if type_label in CAS_MESH_TYPES:
        return "High", "Divergent Mesh Collision", "Same scenegraph resource key but different payloads. This is a strong sign of conflicting mesh definitions."
    if type_label in {"GZPS", "3IDR"}:
        return "High", "Property Set Collision", "Same CAS property-set resource key with different payloads. This can break linkage between recolors and their intended meshes."
    if type_label in {"TXMT", "TXTR"}:
        return "Medium", "Material or Texture Override", "Same material or texture resource key with different payloads. This can be intentional, but it can also create hard-to-explain visual overrides."
    return "Medium", "CAS Asset Collision", "Shared CAS resource key with differing payloads."


def classify_candidate_strength(score: int) -> str:
    if score >= 12:
        return "Strong"
    if score >= 7:
        return "Moderate"
    return "Weak"


def fetch_cas_shared_asset_groups(connection: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = connection.execute(
        """
        SELECT
          pr.resource_key,
          MAX(pr.type_label) AS type_label,
          COUNT(DISTINCT pr.file_id) AS file_count,
          COUNT(DISTINCT COALESCE(pr.body_sha256, '')) AS variant_count
        FROM package_resources pr
        JOIN files f ON f.id = pr.file_id
        WHERE pr.is_dir_record = 0
          AND pr.type_label IN ('GMDC', 'GMND', 'CRES', 'SHPE', 'TXMT', 'TXTR', '3IDR', 'GZPS')
        GROUP BY pr.resource_key
        HAVING COUNT(DISTINCT pr.file_id) > 1
        ORDER BY variant_count DESC, file_count DESC, pr.resource_key
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    results = []
    for row in rows:
        severity, title, explanation = classify_cas_asset_group(row["type_label"], row["variant_count"])
        results.append(
            {
                "resource_key": row["resource_key"],
                "type_label": row["type_label"],
                "file_count": row["file_count"],
                "variant_count": row["variant_count"],
                "severity": severity,
                "title": title,
                "explanation": explanation,
            }
        )
    return results


def classify_resource_conflict(type_label: str, variant_count: int) -> tuple[str, str]:
    if variant_count <= 1:
        return "Low", "Same resource key and same payload across packages. This often indicates duplicate distribution rather than a real behavioral conflict."
    if type_label in HIGH_RISK_RESOURCE_TYPES:
        return "High", "Same resource key but different payload in a high-impact Sims 2 resource type. This is a strong override or conflict candidate."
    return "Medium", "Same resource key with different payload across packages. This is likely an override and should be reviewed."


def render_review_controls(left_file_id: int, right_file_id: int, return_to: str, status: str = "") -> str:
    confirm_query = urlencode({"left": left_file_id, "right": right_file_id, "status": "confirmed", "return_to": return_to})
    dismiss_query = urlencode({"left": left_file_id, "right": right_file_id, "status": "dismissed", "return_to": return_to})
    clear_query = urlencode({"left": left_file_id, "right": right_file_id, "return_to": return_to})
    links = []
    if status != "confirmed":
        links.append(f'<a href="/pair-review?{confirm_query}">Confirm Conflict</a>')
    if status != "dismissed":
        links.append(f'<a href="/pair-review?{dismiss_query}">Dismiss Pair</a>')
    if status:
        links.append(f'<a href="/pair-review/clear?{clear_query}">Clear Review</a>')
    return f'<div class="actions">{"".join(links)}</div>'


@app.get("/", response_class=HTMLResponse)
def home(q: str = "", category: str = "") -> HTMLResponse:
    connection = get_connection()
    stats = fetch_stats(connection)
    files = fetch_recent_files(connection, query=q)
    duplicate_groups = fetch_duplicate_groups(connection)
    bhav_heavy_files = fetch_bhav_heavy_files(connection)
    ttab_heavy_files = fetch_ttab_heavy_files(connection)
    package_profiles = fetch_package_profiles(connection)
    category_breakdown = fetch_category_breakdown(connection)
    metadata_map = fetch_package_metadata_map(connection)
    creator_clusters = fetch_creator_clusters(connection)
    review_map = fetch_review_map(connection)

    duplicate_cards = []
    for group in duplicate_groups:
        group_files = fetch_duplicate_files(connection, group["sha256"])
        review_block = ""
        if len(group_files) >= 2:
            key = pair_key(group_files[0]["id"], group_files[1]["id"])
            review = review_map.get(key)
            review_block = render_review_controls(group_files[0]["id"], group_files[1]["id"], "/", review["status"] if review else "")
        duplicate_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip high">{group['file_count']} files</span>
                <span class="chip">{group['total_size_bytes']} bytes</span>
              </div>
              <p class="summary">Hash: <code>{html.escape(group['sha256'][:16])}...</code></p>
              <ul>
                {''.join(f'<li>{html.escape(file["relative_path"])}</li>' for file in group_files[:4])}
              </ul>
              {review_block}
            </article>
            """
        )

    file_cards = []
    for file in files:
        file_cards.append(
            f"""
            <article class="card">
              <h3>{html.escape(file['file_name'])}</h3>
              <div class="meta">
                <span class="chip">{html.escape(file['extension']) or '[no ext]'}</span>
                <span class="chip">{'Package' if file['is_package'] else 'Support file'}</span>
                <span class="chip">{file['size_bytes']} bytes</span>
                <span class="chip">{html.escape(file['parse_status'])}</span>
                <span class="chip">{file['resource_count']} resources</span>
              </div>
              <p class="summary">{html.escape(file['relative_path'])}</p>
            </article>
            """
        )

    package_cards = []
    visible_profiles = [
        profile
        for profile in package_profiles
        if (
            (not q.strip() or q.strip().lower() in profile["relative_path"].lower() or q.strip().lower() in profile["category"].lower())
            and (not category.strip() or profile["category"] == category)
        )
    ]
    for profile in visible_profiles[:40]:
        gzps_rows = metadata_map.get(profile["id"], [])
        gzps_line = ""
        if gzps_rows:
            first = gzps_rows[0]
            bits = [first["name"] or "[unnamed CAS item]"]
            if first["creator"]:
                bits.append(first["creator"])
            if first["type_value"]:
                bits.append(first["type_value"])
            gzps_line = f'<p class="summary">CAS metadata: {html.escape(" | ".join(bits))}</p>'
        package_cards.append(
            f"""
            <article class="card">
              <h3><a href="/package/{profile['id']}">{html.escape(profile['file_name'])}</a></h3>
              <div class="meta">
                <span class="chip">{html.escape(profile['category'])}</span>
                <span class="chip">{profile['resource_count']} resources</span>
                <span class="chip">{html.escape(profile['parse_status'])}</span>
              </div>
              <p class="summary">{html.escape(profile['relative_path'])}</p>
              <p class="summary">{html.escape(profile['explanation'])}</p>
              {gzps_line}
              <div class="chips">
                {''.join(f'<span class="chip">{html.escape(resource_type)}</span>' for resource_type in profile['resource_types'][:8])}
              </div>
              <div class="actions"><a href="/package/{profile['id']}">Details</a></div>
            </article>
            """
        )

    category_cards = []
    for category, count in category_breakdown:
        category_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip">{html.escape(category)}</span>
              </div>
              <strong>{count}</strong>
            </article>
            """
        )

    body = f"""
      <section class="hero">
        <h1>Sims 2 CC Diagnostics</h1>
        <p>Inventory, duplicate detection, and package-resource conflict diagnostics for The Sims 2 custom content. Parsed `.package` files now contribute DBPF resource keys for stronger conflict analysis.</p>
        <div class="hero-actions">
          <a href="/">Inventory</a>
          <a href="/health">Health</a>
          <a href="/dependencies">Dependencies</a>
          <a href="/crashes">Crashes</a>
          <a href="/conflicts">Resource Conflicts</a>
          <a href="/reviews">Manual Reviews</a>
        </div>
      </section>
      <div class="layout">
        <aside class="panel">
          <div class="panel-inner">
            <div class="stat-grid">
              <div class="stat"><span class="subtle">Files</span><strong>{stats['file_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Packages</span><strong>{stats['package_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Parsed</span><strong>{stats['parsed_package_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Dup Groups</span><strong>{stats['duplicate_group_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Resources</span><strong>{stats['resource_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Conflict Keys</span><strong>{stats['resource_conflict_group_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">OBJDs</span><strong>{stats['objd_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">BHAVs</span><strong>{stats['bhav_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">TTABs</span><strong>{stats['ttab_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">GZPS</span><strong>{stats['gzps_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Crashes</span><strong>{stats['crash_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Config Logs</span><strong>{stats['config_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">GUID Conflicts</span><strong>{stats['objd_guid_conflict_count'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Confirmed Pairs</span><strong>{stats['confirmed_pairs'] or 0}</strong></div>
              <div class="stat"><span class="subtle">Roots</span><strong>{stats['roots_scanned'] or 0}</strong></div>
            </div>
          </div>
        </aside>
        <main class="panel">
          <div class="panel-inner">
            <div class="meta" style="justify-content:space-between; align-items:center;">
              <form method="get" action="/" style="display:flex; gap:10px; width:100%;">
                <input type="text" name="q" value="{html.escape(q)}" placeholder="Search by file name or path" style="flex:1; padding:10px 12px; border:1px solid var(--line); border-radius:12px;">
                <input type="text" name="category" value="{html.escape(category)}" placeholder="Filter category" style="flex:1; padding:10px 12px; border:1px solid var(--line); border-radius:12px;">
                <button type="submit" style="padding:10px 14px; border:1px solid var(--line); border-radius:12px; background:#fff;">Search</button>
              </form>
            </div>
            <div class="grid-2">
              <section class="card">
                <h3>Duplicate Groups</h3>
                <div class="mod-list">
                  {''.join(duplicate_cards) if duplicate_cards else '<p class="summary">No duplicate groups found yet.</p>'}
                </div>
              </section>
              <section class="card">
                <h3>Package Classification</h3>
                <div class="mod-list">
                  {''.join(category_cards) if category_cards else '<p class="summary">No package classifications yet.</p>'}
                </div>
              </section>
              <section class="card">
                <h3>Behavior-Heavy Packages</h3>
                <div class="mod-list">
                  {''.join(f'<article class="card"><h3>{html.escape(row["relative_path"])}</h3><div class="meta"><span class="chip">{row["bhav_count"]} BHAVs</span><span class="chip">{row["total_instructions"]} instructions</span><span class="chip">max {row["max_instruction_count"]}</span></div></article>' for row in bhav_heavy_files[:10]) or '<p class="summary">No parsed BHAV data yet.</p>'}
                </div>
              </section>
              <section class="card">
                <h3>Interaction-Table Packages</h3>
                <div class="mod-list">
                  {''.join(f'<article class="card"><h3>{html.escape(row["relative_path"])}</h3><div class="meta"><span class="chip">{row["ttab_count"]} TTABs</span><span class="chip">{row["linked_objd_count"]} linked OBJDs</span></div></article>' for row in ttab_heavy_files[:10]) or '<p class="summary">No parsed TTAB data yet.</p>'}
                </div>
              </section>
              <section class="card">
                <h3>Classified Packages</h3>
                <div class="mod-list">
                  {''.join(package_cards) if package_cards else '<p class="summary">No matching package profiles.</p>'}
                </div>
              </section>
              <section class="card">
                <h3>Creator Clusters</h3>
                <div class="mod-list">
                  {''.join(
                      f'<article class="card"><div class="meta"><span class="chip">{html.escape(cluster["creator"])}</span><span class="chip">{cluster["package_count"]} packages</span><span class="chip">{cluster["item_count"]} items</span></div>'
                      f'<p class="summary">{html.escape(cluster["packages"][0]["relative_path"])}</p></article>'
                      for cluster in creator_clusters[:10]
                  ) or '<p class="summary">No repeated creators found in parsed GZPS metadata.</p>'}
                </div>
              </section>
              <section class="card">
                <h3>Catalog</h3>
                <div class="mod-list">
                  {''.join(file_cards) if file_cards else '<p class="summary">No scanned files yet. Run the scanner first.</p>'}
                </div>
              </section>
            </div>
          </div>
        </main>
      </div>
    """
    return page("Sims 2 CC Diagnostics", body)


@app.get("/reviews", response_class=HTMLResponse)
def reviews() -> HTMLResponse:
    connection = get_connection()
    rows = connection.execute(
        """
        SELECT pr.left_file_id, pr.right_file_id, pr.status, pr.updated_at,
               lf.relative_path AS left_path, rf.relative_path AS right_path
        FROM pair_reviews pr
        JOIN files lf ON lf.id = pr.left_file_id
        JOIN files rf ON rf.id = pr.right_file_id
        ORDER BY pr.updated_at DESC
        """
    ).fetchall()

    cards = []
    for row in rows:
        cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip {'high' if row['status'] == 'confirmed' else 'low'}">{html.escape(row['status'].title())}</span>
                <span class="chip">{html.escape(row['updated_at'])}</span>
              </div>
              <p class="summary">{html.escape(row['left_path'])}</p>
              <p class="summary">{html.escape(row['right_path'])}</p>
              {render_review_controls(row['left_file_id'], row['right_file_id'], '/reviews', row['status'])}
            </article>
            """
        )

    body = f"""
      <section class="hero">
        <h1>Manual Reviews</h1>
        <p>Persistent duplicate or conflict judgments for Sims 2 custom content pairs.</p>
        <div class="hero-actions">
          <a href="/">Inventory</a>
          <a href="/dependencies">Dependencies</a>
          <a href="/conflicts">Resource Conflicts</a>
        </div>
      </section>
      <div class="panel" style="margin-top:20px;">
        <div class="panel-inner">
          <div class="mod-list">
            {''.join(cards) if cards else '<div class="card"><p class="summary">No manual reviews recorded yet.</p></div>'}
          </div>
        </div>
      </div>
    """
    return page("Manual Reviews", body)


@app.get("/conflicts", response_class=HTMLResponse)
def conflicts() -> HTMLResponse:
    connection = get_connection()
    groups = fetch_resource_conflict_groups(connection)
    type_rows = fetch_resource_type_breakdown(connection)
    guid_rows = fetch_objd_guid_conflicts(connection)
    ttab_rows = fetch_ttab_links(connection)
    review_map = fetch_review_map(connection)

    group_cards = []
    for group in groups:
        files = fetch_resource_conflict_files(connection, group["resource_key"])
        severity, explanation = classify_resource_conflict(group["type_label"], group["variant_count"])
        review_block = ""
        if len(files) >= 2:
            key = pair_key(files[0]["id"], files[1]["id"])
            review = review_map.get(key)
            review_block = render_review_controls(files[0]["id"], files[1]["id"], "/conflicts", review["status"] if review else "")
        group_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip {'high' if severity == 'High' else 'low' if severity == 'Low' else ''}">{html.escape(group['type_label'])}</span>
                <span class="chip">{group['file_count']} packages</span>
                <span class="chip {'high' if severity == 'High' else 'low' if severity == 'Low' else ''}">{severity}</span>
                <span class="chip">{group['variant_count']} payload variants</span>
              </div>
              <p class="summary"><code>{html.escape(group['resource_key'])}</code></p>
              <p class="summary">{html.escape(explanation)}</p>
              <ul>
                {''.join(f'<li>{html.escape(file["relative_path"])} <span class="subtle">offset={file["file_offset"]} size={file["file_size"]} body={html.escape((file["body_sha256"] or "")[:12])}</span></li>' for file in files[:6])}
              </ul>
              {review_block}
            </article>
            """
        )

    type_cards = []
    for row in type_rows:
        type_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip">{html.escape(row['type_label'])}</span>
              </div>
              <strong>{row['resource_count']}</strong>
            </article>
            """
        )

    guid_cards = []
    for row in guid_rows:
        files = fetch_objd_conflict_files(connection, row["guid"])
        review_block = ""
        if len(files) >= 2:
            key = pair_key(files[0]["file_id"], files[1]["file_id"])
            review = review_map.get(key)
            review_block = render_review_controls(files[0]["file_id"], files[1]["file_id"], "/conflicts", review["status"] if review else "")
        guid_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip high">OBJD GUID</span>
                <span class="chip">{row['file_count']} packages</span>
                <span class="chip">0x{row['guid']:08X}</span>
              </div>
              <p class="summary">Multiple object definitions claim the same GUID. In Sims 2 this is a high-value conflict signal because object identity can collide even when package filenames look unrelated.</p>
              <ul>
                {''.join(f'<li>{html.escape(file["relative_path"])} <span class="subtle">{html.escape(file["object_name"] or "[unnamed object]")} price={file["price"] if file["price"] is not None else "?"} type={file["object_type"] if file["object_type"] is not None else "?"}</span></li>' for file in files[:6])}
              </ul>
              {review_block}
            </article>
            """
        )

    ttab_cards = []
    for row in ttab_rows:
        names = row["object_names"] or ""
        ttab_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip">TTAB</span>
                <span class="chip">instance 0x{row['instance_id']:08X}</span>
                <span class="chip">{row['linked_objd_count']} linked OBJDs</span>
                <span class="chip">format {row['format_code'] if row['format_code'] is not None else '?'}</span>
              </div>
              <p class="summary">{html.escape(row['relative_path'])}</p>
              <p class="summary">{html.escape(names if names else 'No linked object names resolved yet.')}</p>
            </article>
            """
        )

    body = f"""
      <section class="hero">
        <h1>Resource Conflicts</h1>
        <p>This page groups parsed Sims 2 package resources by shared DBPF resource keys and parsed object GUIDs. Shared GUIDs are especially important because they point at object identity collisions, not just raw override behavior.</p>
        <div class="hero-actions">
          <a href="/">Inventory</a>
          <a href="/dependencies">Dependencies</a>
          <a href="/reviews">Manual Reviews</a>
        </div>
      </section>
      <div class="layout">
        <aside class="panel">
          <div class="panel-inner">
            <h3>Top Resource Types</h3>
            <div class="mod-list">
              {''.join(type_cards) if type_cards else '<div class="card"><p class="summary">No parsed resources yet.</p></div>'}
            </div>
          </div>
        </aside>
        <main class="panel">
          <div class="panel-inner">
            <h3>OBJD GUID Collisions</h3>
            <div class="mod-list">
              {''.join(guid_cards) if guid_cards else '<div class="card"><p class="summary">No parsed OBJD GUID collisions detected yet.</p></div>'}
            </div>
            <h3 style="margin-top:20px;">Object To TTAB Links</h3>
            <div class="mod-list">
              {''.join(ttab_cards) if ttab_cards else '<div class="card"><p class="summary">No TTAB to OBJD links detected yet.</p></div>'}
            </div>
            <h3 style="margin-top:20px;">Shared Resource Keys</h3>
            <div class="mod-list">
              {''.join(group_cards) if group_cards else '<div class="card"><p class="summary">No shared resource keys detected yet.</p></div>'}
            </div>
          </div>
        </main>
      </div>
    """
    return page("Resource Conflicts", body)


@app.get("/pair-review")
def pair_review(left: int, right: int, status: str, return_to: str = "/") -> RedirectResponse:
    if status in {"confirmed", "dismissed"}:
        save_review(left, right, status)
    return RedirectResponse(url=return_to, status_code=303)


@app.get("/pair-review/clear")
def pair_review_clear(left: int, right: int, return_to: str = "/") -> RedirectResponse:
    clear_review(left, right)
    return RedirectResponse(url=return_to, status_code=303)


@app.get("/package/{file_id}", response_class=HTMLResponse)
def package_detail(file_id: int) -> HTMLResponse:
    connection = get_connection()
    detail = fetch_package_detail(connection, file_id)
    if not detail:
        return page("Package Not Found", '<section class="hero"><h1>Package Not Found</h1><p>No package exists with that id.</p><div class="hero-actions"><a href="/">Inventory</a></div></section>')
    dependency_candidates, _, _ = fetch_dependency_candidates(connection)
    reverse_dependents = []
    for recolor_id, candidates in dependency_candidates.items():
        for candidate in candidates:
            if candidate["mesh_id"] == file_id:
                recolor_detail = fetch_file_row(connection, recolor_id)
                if recolor_detail:
                    reverse_dependents.append((recolor_detail, candidate))
                break
    top_candidates = dependency_candidates.get(file_id, [])
    gzps_cards = []
    for row in detail["gzps"]:
        gzps_cards.append(
            f"""
            <article class="card">
              <div class="chips">
                <span class="chip">{html.escape(row['type_value'] or 'unknown')}</span>
                <span class="chip">age {row['age'] if row['age'] is not None else '?'}</span>
                <span class="chip">gender {row['gender'] if row['gender'] is not None else '?'}</span>
                <span class="chip">parts {row['parts'] if row['parts'] is not None else '?'}</span>
              </div>
              <p class="summary">{html.escape(row['name'] or '[unnamed item]')}</p>
              <p class="summary">{html.escape(row['creator'] or '[unknown creator]')}</p>
              <p class="summary">family: {html.escape(row['family'] or '[none]')}</p>
            </article>
            """
        )
    resource_cards = [
        f'<span class="chip">{html.escape(type_label)} x{count}</span>'
        for type_label, count in detail["type_counts"]
    ]
    dependency_cards = []
    for candidate in top_candidates[:5]:
        dependency_cards.append(
            f"""
            <article class="card">
              <h3><a href="/package/{candidate['mesh_id']}">{html.escape(candidate['mesh_name'])}</a></h3>
              <div class="meta">
                <span class="chip">{classify_candidate_strength(candidate['score'])}</span>
                <span class="chip">score {candidate['score']}</span>
                <span class="chip">{len(candidate['explicit_refs'])} explicit refs</span>
              </div>
              <p class="summary">{html.escape(candidate['mesh_path'])}</p>
              <p class="summary">{html.escape(', '.join(candidate['reasons']))}</p>
            </article>
            """
        )
    reverse_cards = []
    for recolor_detail, candidate in reverse_dependents[:12]:
        reverse_cards.append(
            f"""
            <article class="card">
              <h3><a href="/package/{recolor_detail['id']}">{html.escape(recolor_detail['file_name'])}</a></h3>
              <div class="meta">
                <span class="chip">{classify_candidate_strength(candidate['score'])}</span>
                <span class="chip">score {candidate['score']}</span>
              </div>
              <p class="summary">{html.escape(recolor_detail['relative_path'])}</p>
            </article>
            """
        )
    compare_link = ""
    if top_candidates:
        compare_link = f'<a href="/compare?left={file_id}&right={top_candidates[0]["mesh_id"]}">Compare With Top Match</a>'
    body = f"""
      <section class="hero">
        <h1>{html.escape(detail['file']['file_name'])}</h1>
        <p>{html.escape(detail['file']['relative_path'])}</p>
        <div class="hero-actions">
          <a href="/">Inventory</a>
          <a href="/health">Health</a>
          <a href="/dependencies">Dependencies</a>
          <a href="/conflicts">Resource Conflicts</a>
          {compare_link}
        </div>
      </section>
      <div class="grid-2">
        <section class="card">
          <h3>Profile</h3>
          <div class="chips">
            <span class="chip">{html.escape(detail['profile']['category'] if detail['profile'] else 'Unknown')}</span>
            <span class="chip">{detail['file']['resource_count']} resources</span>
            <span class="chip">{html.escape(detail['file']['parse_status'])}</span>
            <span class="chip">{detail['file']['size_bytes']} bytes</span>
          </div>
          <p class="summary">{html.escape(detail['profile']['explanation'] if detail['profile'] else 'No profile available.')}</p>
          <p class="summary">sha256: <code>{html.escape(detail['file']['sha256'][:20])}...</code></p>
          <div class="chips">{''.join(resource_cards)}</div>
        </section>
        <section class="card">
          <h3>CAS Metadata</h3>
          <div class="mod-list">
            {''.join(gzps_cards) if gzps_cards else '<div class="card"><p class="summary">No parsed GZPS metadata for this package.</p></div>'}
          </div>
        </section>
        <section class="card">
          <h3>Depends On</h3>
          <div class="mod-list">
            {''.join(dependency_cards) if dependency_cards else '<div class="card"><p class="summary">No dependency candidates recorded for this package.</p></div>'}
          </div>
        </section>
        <section class="card">
          <h3>Referenced By</h3>
          <div class="mod-list">
            {''.join(reverse_cards) if reverse_cards else '<div class="card"><p class="summary">No other package currently resolves to this one as a dependency target.</p></div>'}
          </div>
        </section>
      </div>
    """
    return page(f"Package {detail['file']['file_name']}", body)


@app.get("/compare", response_class=HTMLResponse)
def compare(left: int, right: int) -> HTMLResponse:
    connection = get_connection()
    left_detail = fetch_package_detail(connection, left)
    right_detail = fetch_package_detail(connection, right)
    if not left_detail or not right_detail:
        return page("Compare", '<section class="hero"><h1>Compare</h1><p>Both package ids must exist.</p><div class="hero-actions"><a href="/">Inventory</a></div></section>')
    left_types = {label for label, _ in left_detail["type_counts"]}
    right_types = {label for label, _ in right_detail["type_counts"]}
    shared_types = sorted(left_types & right_types)
    left_only = sorted(left_types - right_types)
    right_only = sorted(right_types - left_types)
    dependency_candidates, _, _ = fetch_dependency_candidates(connection)
    directional = next((candidate for candidate in dependency_candidates.get(left, []) if candidate["mesh_id"] == right), None)
    reverse_directional = next((candidate for candidate in dependency_candidates.get(right, []) if candidate["mesh_id"] == left), None)
    relation_summary = []
    if directional:
        relation_summary.append(f"{left_detail['file']['file_name']} depends on {right_detail['file']['file_name']} with score {directional['score']}.")
    if reverse_directional:
        relation_summary.append(f"{right_detail['file']['file_name']} depends on {left_detail['file']['file_name']} with score {reverse_directional['score']}.")
    if not relation_summary:
        relation_summary.append("No direct dependency edge is currently recorded between these packages.")
    def render_detail(detail: dict) -> str:
        gzps = detail["gzps"]
        return f"""
        <article class="card">
          <h3><a href="/package/{detail['file']['id']}">{html.escape(detail['file']['file_name'])}</a></h3>
          <p class="summary">{html.escape(detail['file']['relative_path'])}</p>
          <div class="chips">
            <span class="chip">{html.escape(detail['profile']['category'] if detail['profile'] else 'Unknown')}</span>
            {''.join(f'<span class="chip">{html.escape(label)} x{count}</span>' for label, count in detail['type_counts'])}
          </div>
          <div class="mod-list" style="margin-top:12px;">
            {''.join(f'<div class="card"><p class="summary">{html.escape(row["name"] or "[unnamed]")}</p><p class="summary">{html.escape(row["creator"] or "[unknown creator]")}</p></div>' for row in gzps[:6]) or '<div class="card"><p class="summary">No GZPS metadata.</p></div>'}
          </div>
        </article>
        """
    body = f"""
      <section class="hero">
        <h1>Compare Packages</h1>
        <p>Side-by-side comparison of package types, CAS metadata, and dependency signals.</p>
        <div class="hero-actions">
          <a href="/">Inventory</a>
          <a href="/health">Health</a>
          <a href="/package/{left}">Left Package</a>
          <a href="/package/{right}">Right Package</a>
        </div>
      </section>
      <div class="grid-2">
        {render_detail(left_detail)}
        {render_detail(right_detail)}
        <section class="card">
          <h3>Relationship</h3>
          <div class="mod-list">
            {''.join(f'<div class="card"><p class="summary">{html.escape(line)}</p></div>' for line in relation_summary)}
          </div>
        </section>
        <section class="card">
          <h3>Type Comparison</h3>
          <p class="summary">Shared: {html.escape(', '.join(shared_types) if shared_types else '[none]')}</p>
          <p class="summary">Left only: {html.escape(', '.join(left_only) if left_only else '[none]')}</p>
          <p class="summary">Right only: {html.escape(', '.join(right_only) if right_only else '[none]')}</p>
        </section>
      </div>
    """
    return page("Compare Packages", body)


@app.get("/dependencies", response_class=HTMLResponse)
def dependencies() -> HTMLResponse:
    connection = get_connection()
    groups = fetch_dependency_groups(connection)
    orphan_groups = fetch_orphan_recolor_groups(connection)
    duplicate_mesh_groups = fetch_duplicate_mesh_candidates(connection)
    cas_asset_groups = fetch_cas_shared_asset_groups(connection)
    dependency_candidates, unresolved_recolors, signatures = fetch_dependency_candidates(connection)
    review_map = fetch_review_map(connection)

    cards = []
    for group in groups:
        recolor_blocks = []
        for item in group["recolor_packages"][:12]:
            candidates = dependency_candidates.get(item["id"], [])
            if candidates:
                top = candidates[0]
                strength = classify_candidate_strength(top["score"])
                detail = f'best match: {html.escape(top["mesh_name"])} ({strength.lower()} score {top["score"]})'
                if top["explicit_refs"]:
                    detail += f' with {len(top["explicit_refs"])} explicit 3IDR refs'
                if top["shared_names"]:
                    detail += f' via {html.escape(", ".join(top["shared_names"][:3]))}'
            else:
                detail = "no mesh candidate scored yet"
            recolor_blocks.append(f'<li>{html.escape(item["file_name"])} <span class="subtle">{detail}</span></li>')
        cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip {'high' if group['likely_bundle'] else ''}">{'Likely Mesh Bundle' if group['likely_bundle'] else 'Mixed Folder'}</span>
                <span class="chip">{group['package_count']} packages</span>
                <span class="chip">{len(group['mesh_packages'])} meshes</span>
                <span class="chip">{len(group['recolor_packages'])} recolors</span>
                <span class="chip">score {group['bundle_score']}</span>
              </div>
              <h3>{html.escape(group['folder'])}</h3>
              <p class="summary">This groups files by folder and package role. For your current test set, folder-local mesh-plus-recolor bundles are a strong dependency heuristic because the downloads are distributed as set folders.</p>
              <div class="grid-2">
                <div class="card">
                  <h3>Mesh Packages</h3>
                  <ul>
                    {''.join(f'<li>{html.escape(item["file_name"])}</li>' for item in group['mesh_packages']) or '<li>None</li>'}
                  </ul>
                </div>
                <div class="card">
                  <h3>Recolor Packages</h3>
                  <ul>
                    {''.join(recolor_blocks) or '<li>None</li>'}
                  </ul>
                </div>
              </div>
            </article>
            """
        )

    orphan_cards = []
    for group in orphan_groups:
        orphan_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip high">Orphan Recolor Folder</span>
                <span class="chip">{len(group['recolor_packages'])} recolors</span>
                <span class="chip">{group['package_count']} packages</span>
              </div>
              <h3>{html.escape(group['folder'])}</h3>
              <p class="summary">This folder contains recolor/property packages but no local mesh or hybrid package anchor. That does not prove the set is broken, but it is the strongest current heuristic for a missing mesh dependency.</p>
              <ul>
                {''.join(f'<li>{html.escape(item["file_name"])}</li>' for item in group['recolor_packages'][:12])}
              </ul>
            </article>
            """
        )

    unresolved_cards = []
    for item in unresolved_recolors[:20]:
        signature = signatures.get(item["id"], {})
        unresolved_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip high">Unresolved Recolor</span>
                <span class="chip">{html.escape(item['category'])}</span>
              </div>
              <h3>{html.escape(item['file_name'])}</h3>
              <p class="summary">{html.escape(item['relative_path'])}</p>
              <p class="summary">No strong mesh candidate scored from current scenegraph-name extraction. This is a stronger missing-dependency signal than folder-only orphan detection, although it can still mean the package naming is simply opaque.</p>
              <div class="chips">
                {''.join(f'<span class="chip">{html.escape(name)}</span>' for name in signature.get('normalized_names', [])[:6]) or '<span class="chip">No extracted names</span>'}
              </div>
            </article>
            """
        )

    duplicate_mesh_cards = []
    for group in duplicate_mesh_groups:
        review_block = ""
        if len(group["items"]) >= 2:
            key = pair_key(group["items"][0]["id"], group["items"][1]["id"])
            review = review_map.get(key)
            review_block = render_review_controls(group["items"][0]["id"], group["items"][1]["id"], "/dependencies", review["status"] if review else "")
        duplicate_mesh_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip {'low' if group['exact_duplicate'] else 'high'}">{'Exact Duplicate Mesh' if group['exact_duplicate'] else 'Variant Mesh Duplicate'}</span>
                <span class="chip">{group['copy_count']} copies</span>
                <span class="chip">{group['variant_count']} hash variants</span>
                <span class="chip {'low' if group['severity'] == 'Low' else 'high'}">{group['severity']}</span>
              </div>
              <h3>{html.escape(group['file_name'])}</h3>
              <p class="summary">{html.escape(group['explanation'])}</p>
              <ul>
                {''.join(f'<li>{html.escape(item["relative_path"])} <span class="subtle">{html.escape(item["sha256"][:12])}</span></li>' for item in group['items'])}
              </ul>
              <div class="actions"><a href="/compare?left={group['items'][0]['id']}&right={group['items'][1]['id']}">Compare Copies</a></div>
              {review_block}
            </article>
            """
        )

    cas_asset_cards = []
    for group in cas_asset_groups:
        files = fetch_resource_conflict_files(connection, group["resource_key"])
        review_block = ""
        if len(files) >= 2:
            key = pair_key(files[0]["id"], files[1]["id"])
            review = review_map.get(key)
            review_block = render_review_controls(files[0]["id"], files[1]["id"], "/dependencies", review["status"] if review else "")
        cas_asset_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip {'high' if group['severity'] == 'High' else 'low' if group['severity'] == 'Low' else ''}">{html.escape(group['title'])}</span>
                <span class="chip">{html.escape(group['type_label'])}</span>
                <span class="chip">{group['file_count']} packages</span>
                <span class="chip">{group['variant_count']} payload variants</span>
              </div>
              <p class="summary"><code>{html.escape(group['resource_key'])}</code></p>
              <p class="summary">{html.escape(group['explanation'])}</p>
              <ul>
                {''.join(f'<li>{html.escape(file["relative_path"])} <span class="subtle">body={html.escape((file["body_sha256"] or "")[:12])}</span></li>' for file in files[:6])}
              </ul>
              {review_block}
            </article>
            """
        )

    body = f"""
      <section class="hero">
        <h1>Dependencies</h1>
        <p>This page focuses on CAS-style dependencies: likely mesh bundles, recolor folders with no local dependency anchor, duplicate mesh packages, and shared assets classified as benign duplication versus more suspicious collisions.</p>
        <div class="hero-actions">
          <a href="/">Inventory</a>
          <a href="/health">Health</a>
          <a href="/conflicts">Resource Conflicts</a>
          <a href="/reviews">Manual Reviews</a>
        </div>
      </section>
      <div class="panel" style="margin-top:20px;">
        <div class="panel-inner">
          <div class="grid-2">
            <section class="card">
              <h3>Likely Mesh Bundles</h3>
              <div class="mod-list">
                {''.join(cards) if cards else '<div class="card"><p class="summary">No likely dependency bundles detected yet.</p></div>'}
              </div>
            </section>
            <section class="card">
              <h3>Orphan Recolors</h3>
              <div class="mod-list">
                {''.join(orphan_cards) if orphan_cards else '<div class="card"><p class="summary">No orphan recolor folders detected in the current dataset.</p></div>'}
              </div>
            </section>
            <section class="card">
              <h3>Unresolved Recolors</h3>
              <div class="mod-list">
                {''.join(unresolved_cards) if unresolved_cards else '<div class="card"><p class="summary">Every recolor package found at least one mesh candidate from the current linkage heuristics.</p></div>'}
              </div>
            </section>
            <section class="card">
              <h3>Duplicate Mesh Packages</h3>
              <div class="mod-list">
                {''.join(duplicate_mesh_cards) if duplicate_mesh_cards else '<div class="card"><p class="summary">No duplicate mesh-package names detected yet.</p></div>'}
              </div>
            </section>
            <section class="card">
              <h3>Shared CAS Assets</h3>
              <div class="mod-list">
                {''.join(cas_asset_cards) if cas_asset_cards else '<div class="card"><p class="summary">No shared CAS asset groups detected yet.</p></div>'}
              </div>
            </section>
          </div>
        </div>
      </div>
    """
    return page("Dependencies", body)


@app.get("/health", response_class=HTMLResponse)
def health() -> HTMLResponse:
    connection = get_connection()
    dependency_candidates, unresolved_recolors, signatures = fetch_dependency_candidates(connection)
    low_confidence = []
    for recolor_id, candidates in dependency_candidates.items():
        if not candidates:
            continue
        top = candidates[0]
        if top["score"] >= 12:
            continue
        file_row = fetch_file_row(connection, recolor_id)
        if file_row:
            low_confidence.append((file_row, top, signatures.get(recolor_id, {})))
    low_confidence.sort(key=lambda item: (item[1]["score"], item[0]["relative_path"]))
    duplicate_mesh_groups = fetch_duplicate_mesh_candidates(connection)
    material_overrides = fetch_material_override_groups(connection)
    partial_gzps = fetch_partial_gzps_rows(connection)

    low_conf_cards = []
    for file_row, top, signature in low_confidence[:20]:
        low_conf_cards.append(
            f"""
            <article class="card">
              <h3><a href="/package/{file_row['id']}">{html.escape(file_row['file_name'])}</a></h3>
              <div class="meta">
                <span class="chip">{classify_candidate_strength(top['score'])}</span>
                <span class="chip">score {top['score']}</span>
              </div>
              <p class="summary">{html.escape(file_row['relative_path'])}</p>
              <p class="summary">Top candidate: {html.escape(top['mesh_name'])}</p>
              <div class="chips">
                {''.join(f'<span class="chip">{html.escape(token)}</span>' for token in signature.get('tokens', [])[:8])}
              </div>
            </article>
            """
        )

    unresolved_cards = []
    for profile in unresolved_recolors[:20]:
        unresolved_cards.append(
            f"""
            <article class="card">
              <h3><a href="/package/{profile['id']}">{html.escape(profile['file_name'])}</a></h3>
              <p class="summary">{html.escape(profile['relative_path'])}</p>
              <p class="summary">No dependency candidate cleared the current matching threshold.</p>
            </article>
            """
        )

    override_cards = []
    for row in material_overrides[:20]:
        files = fetch_resource_conflict_files(connection, row["resource_key"])
        override_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip high">{html.escape(row['type_label'])}</span>
                <span class="chip">{row['file_count']} packages</span>
                <span class="chip">{row['variant_count']} variants</span>
              </div>
              <p class="summary"><code>{html.escape(row['resource_key'])}</code></p>
              <ul>
                {''.join(f'<li>{html.escape(file["relative_path"])}</li>' for file in files[:6])}
              </ul>
            </article>
            """
        )

    partial_cards = []
    for row in partial_gzps[:20]:
        partial_cards.append(
            f"""
            <article class="card">
              <h3><a href="/package/{row['id']}">{html.escape(Path(row['relative_path']).name)}</a></h3>
              <p class="summary">{html.escape(row['relative_path'])}</p>
              <div class="chips">
                <span class="chip">{html.escape(row['name'] or '[missing name]')}</span>
                <span class="chip">{html.escape(row['creator'] or '[missing creator]')}</span>
                <span class="chip">{html.escape(row['family'] or '[missing family]')}</span>
              </div>
            </article>
            """
        )

    duplicate_cards = []
    for group in duplicate_mesh_groups[:20]:
        duplicate_cards.append(
            f"""
            <article class="card">
              <h3>{html.escape(group['file_name'])}</h3>
              <div class="meta">
                <span class="chip {'low' if group['exact_duplicate'] else 'high'}">{'Exact duplicate' if group['exact_duplicate'] else 'Variant duplicate'}</span>
                <span class="chip">{group['copy_count']} copies</span>
              </div>
              <p class="summary">{html.escape(group['explanation'])}</p>
              <div class="actions"><a href="/compare?left={group['items'][0]['id']}&right={group['items'][1]['id']}">Compare</a></div>
            </article>
            """
        )

    body = f"""
      <section class="hero">
        <h1>Content Health</h1>
        <p>This page concentrates the remaining weak points in the scanned content: low-confidence dependency matches, unresolved recolors, material overrides, partial CAS metadata, and duplicate mesh distribution.</p>
        <div class="hero-actions">
          <a href="/">Inventory</a>
          <a href="/dependencies">Dependencies</a>
          <a href="/conflicts">Resource Conflicts</a>
        </div>
      </section>
      <div class="grid-2">
        <section class="card">
          <h3>Low-Confidence Dependency Links</h3>
          <div class="mod-list">
            {''.join(low_conf_cards) if low_conf_cards else '<div class="card"><p class="summary">No low-confidence dependency matches in the current dataset.</p></div>'}
          </div>
        </section>
        <section class="card">
          <h3>Unresolved Recolors</h3>
          <div class="mod-list">
            {''.join(unresolved_cards) if unresolved_cards else '<div class="card"><p class="summary">No unresolved recolors in the current dataset.</p></div>'}
          </div>
        </section>
        <section class="card">
          <h3>Material Overrides</h3>
          <div class="mod-list">
            {''.join(override_cards) if override_cards else '<div class="card"><p class="summary">No shared TXMT/TXTR resource keys with divergent payloads were detected.</p></div>'}
          </div>
        </section>
        <section class="card">
          <h3>Partial CAS Metadata</h3>
          <div class="mod-list">
            {''.join(partial_cards) if partial_cards else '<div class="card"><p class="summary">All parsed GZPS rows currently have name, creator, and family populated.</p></div>'}
          </div>
        </section>
        <section class="card">
          <h3>Duplicate Mesh Distribution</h3>
          <div class="mod-list">
            {''.join(duplicate_cards) if duplicate_cards else '<div class="card"><p class="summary">No duplicate mesh-package groups detected.</p></div>'}
          </div>
        </section>
      </div>
    """
    return page("Content Health", body)


@app.get("/crashes", response_class=HTMLResponse)
def crashes() -> HTMLResponse:
    connection = get_connection()
    reports = fetch_crash_reports(connection)
    crash_cards = []
    for report in reports:
        notes = correlate_crash_report(connection, report)
        crash_cards.append(
            f"""
            <article class="card">
              <div class="meta">
                <span class="chip {'high' if report['log_type'] == 'crash' else 'low'}">{html.escape(report['log_type'])}</span>
                <span class="chip">{html.escape(report['crash_category'])}</span>
                <span class="chip">{html.escape(report['file_name'])}</span>
              </div>
              <p class="summary">{html.escape(report['summary'])}</p>
              <p class="summary">module: {html.escape(report['exception_module'] or '[unknown]')} | code: {html.escape(report['exception_code'] or '[unknown]')} | address: {html.escape(report['fault_address'] or '[unknown]')}</p>
              <p class="summary">gpu: {html.escape(report['graphics_device'] or '[unknown]')} | texture memory: {report['texture_memory_mb'] if report['texture_memory_mb'] is not None else '[unknown]'}</p>
              <p class="summary">source: {html.escape(report['source_path'])}</p>
              <div class="mod-list" style="margin-top:12px;">
                {''.join(f'<div class="card"><p class="summary">{html.escape(note)}</p></div>' for note in notes)}
              </div>
            </article>
            """
        )

    body = f"""
      <section class="hero">
        <h1>Crash Correlation</h1>
        <p>This page ingests Sims 2 crash and config logs, classifies them into broad failure types, and correlates them against the scanned custom-content and package diagnostics.</p>
        <div class="hero-actions">
          <a href="/">Inventory</a>
          <a href="/health">Health</a>
          <a href="/dependencies">Dependencies</a>
          <a href="/conflicts">Resource Conflicts</a>
        </div>
      </section>
      <div class="panel" style="margin-top:20px;">
        <div class="panel-inner">
          <div class="mod-list">
            {''.join(crash_cards) if crash_cards else '<div class="card"><p class="summary">No crash or config logs imported yet. Use import_crash_logs.py against a logs folder first.</p></div>'}
          </div>
        </div>
      </div>
    """
    return page("Crash Correlation", body)
