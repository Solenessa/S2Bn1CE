#!/usr/bin/env python3

from __future__ import annotations

import re
import sqlite3
from pathlib import PurePosixPath


HIGH_RISK_RESOURCE_TYPES = {"BHAV", "BCON", "GLOB", "OBJD", "TTAB", "TTAs", "TPRP"}
CAS_MESH_TYPES = {"GMDC", "GMND", "CRES", "SHPE"}
CAS_RECOLOR_TYPES = {"GZPS", "TXMT", "TXTR", "3IDR"}
HACK_TYPES = {"BHAV", "BCON", "GLOB", "OBJD", "TTAB", "TTAs", "TPRP", "TRCN"}


def fetch_schema_version(connection: sqlite3.Connection) -> int:
    return int(connection.execute("PRAGMA user_version").fetchone()[0])


def pair_key(left_file_id: int, right_file_id: int) -> tuple[int, int]:
    if left_file_id <= right_file_id:
        return left_file_id, right_file_id
    return right_file_id, left_file_id


def fetch_review_map(connection: sqlite3.Connection) -> dict[tuple[int, int], sqlite3.Row]:
    rows = connection.execute(
        "SELECT left_file_id, right_file_id, status, note, updated_at FROM pair_reviews"
    ).fetchall()
    return {(row["left_file_id"], row["right_file_id"]): row for row in rows}


def save_review(connection: sqlite3.Connection, left_file_id: int, right_file_id: int, status: str) -> None:
    pair_left, pair_right = pair_key(left_file_id, right_file_id)
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


def clear_review(connection: sqlite3.Connection, left_file_id: int, right_file_id: int) -> None:
    pair_left, pair_right = pair_key(left_file_id, right_file_id)
    connection.execute(
        "DELETE FROM pair_reviews WHERE left_file_id = ? AND right_file_id = ?",
        (pair_left, pair_right),
    )
    connection.commit()


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
          ) AS objd_guid_conflict_count,
          (SELECT COUNT(*) FROM gzps_entries) AS gzps_count,
          (SELECT COUNT(*) FROM crash_reports WHERE log_type = 'crash') AS crash_count,
          (SELECT COUNT(*) FROM crash_reports WHERE log_type = 'config') AS config_count
        FROM files
        """
    ).fetchone()


def fetch_recent_files(connection: sqlite3.Connection, query: str = "", limit: int = 80) -> list[sqlite3.Row]:
    params: list[object] = []
    sql_parts = [
        """
        SELECT id, relative_path, file_name, extension, size_bytes, is_package, sha256, parse_status, resource_count
        FROM files
        """
    ]
    if query.strip():
        like_value = f"%{query.strip().lower()}%"
        sql_parts.append(
            """
        WHERE
          lower(relative_path) LIKE ?
          OR lower(file_name) LIKE ?
          OR lower(extension) LIKE ?
        """
        )
        params.extend([like_value, like_value, like_value])
    sql_parts.append(
        """
        ORDER BY relative_path
        LIMIT ?
        """
    )
    params.append(limit)
    return connection.execute("\n".join(sql_parts), params).fetchall()


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
        return "Mesh Package", "Contains scenegraph or mesh resources and may be a required mesh dependency."
    if has_recolor:
        return "Recolor or Property Set", "Contains recolor/property resources such as GZPS, TXMT, TXTR, or 3IDR."
    if resource_types == {"STR#"} or resource_types == {"CTSS"}:
        return "Text Support Package", "Mostly or entirely string resources."
    return "Unclassified Package", "Needs more specific parsing to classify confidently."


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


def build_scenegraph_signatures(connection: sqlite3.Connection) -> dict[int, dict]:
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
        raw_strings = set()
        normalized_names = set()
        tokens = extract_name_tokens(file_row["file_name"]) | extract_name_tokens(file_row["relative_path"])
        hint_rows = connection.execute(
            """
            SELECT value, normalized_value
            FROM scenegraph_names
            WHERE file_id = ?
            ORDER BY value
            """,
            (file_row["id"],),
        ).fetchall()
        for hint in hint_rows:
            raw_strings.add(hint["value"])
            if hint["normalized_value"]:
                normalized_names.add(hint["normalized_value"])
                tokens |= extract_name_tokens(hint["normalized_value"])
        signatures[file_row["id"]] = {
            "file_id": file_row["id"],
            "relative_path": file_row["relative_path"],
            "file_name": file_row["file_name"],
            "raw_strings": sorted(raw_strings)[:24],
            "normalized_names": sorted(normalized_names),
            "tokens": sorted(tokens),
        }
    return signatures


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
    if "body" in " ".join(overlap):
        score += 2
    return score, reasons


def fetch_dependency_candidates(connection: sqlite3.Connection) -> tuple[dict[int, list[dict]], list[dict], dict[int, dict]]:
    signatures = build_scenegraph_signatures(connection)
    resource_owner_map = build_resource_owner_map(connection)
    profiles = {profile["id"]: profile for profile in fetch_package_profiles(connection, limit=5000)}
    mesh_profiles = [profile for profile in profiles.values() if profile["category"] in {"Mesh Package", "CAS Hybrid Set"}]
    recolor_profiles = [profile for profile in profiles.values() if profile["category"] in {"Recolor or Property Set", "CAS Hybrid Set"}]

    candidates_by_recolor: dict[int, list[dict]] = {}
    unresolved: list[dict] = []
    for recolor in recolor_profiles:
        recolor_sig = signatures.get(recolor["id"], {"normalized_names": [], "raw_strings": [], "relative_path": recolor["relative_path"], "file_name": recolor["file_name"], "tokens": []})
        scored_map: dict[int, dict] = {}
        ref_rows = connection.execute(
            """
            SELECT target_resource_key, target_type_id
            FROM resource_links
            WHERE file_id = ? AND source_type_label = '3IDR'
            """,
            (recolor["id"],),
        ).fetchall()
        material_rows = connection.execute(
            """
            SELECT DISTINCT normalized_value
            FROM scenegraph_names
            WHERE file_id = ? AND source_type_label = 'TXMT' AND normalized_value != ''
            ORDER BY normalized_value
            LIMIT 4
            """,
            (recolor["id"],),
        ).fetchall()
        material_names = [row["normalized_value"] for row in material_rows]
        for ref in ref_rows:
            for owner in resource_owner_map.get(ref["target_resource_key"], []):
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
                candidate["reasons"].append(f"explicit 3IDR ref to {ref['target_type_id']:08X}")
                candidate["explicit_refs"].append(ref["target_resource_key"])
                if material_names:
                    candidate["material_names"] = material_names

        for mesh in mesh_profiles:
            if mesh["id"] == recolor["id"]:
                continue
            mesh_sig = signatures.get(mesh["id"], {"normalized_names": [], "raw_strings": [], "relative_path": mesh["relative_path"], "file_name": mesh["file_name"], "tokens": []})
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
    return [
        {
            "resource_key": row["resource_key"],
            "type_label": row["type_label"],
            "file_count": row["file_count"],
            "variant_count": row["variant_count"],
            "severity": classify_cas_asset_group(row["type_label"], row["variant_count"])[0],
            "title": classify_cas_asset_group(row["type_label"], row["variant_count"])[1],
            "explanation": classify_cas_asset_group(row["type_label"], row["variant_count"])[2],
        }
        for row in rows
    ]


def classify_resource_conflict(type_label: str, variant_count: int) -> tuple[str, str]:
    if variant_count <= 1:
        return "Low", "Same resource key and same payload across packages. This often indicates duplicate distribution rather than a real behavioral conflict."
    if type_label in HIGH_RISK_RESOURCE_TYPES:
        return "High", "Same resource key but different payload in a high-impact Sims 2 resource type. This is a strong override or conflict candidate."
    return "Medium", "Same resource key with different payload across packages. This is likely an override and should be reviewed."


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
