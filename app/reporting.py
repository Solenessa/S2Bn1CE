#!/usr/bin/env python3

from __future__ import annotations

from datetime import datetime
from pathlib import Path

try:
    from app.db import DEFAULT_DB_PATH, connect, resolve_db_path
    from app.diagnostics import (
        classify_candidate_strength,
        correlate_crash_report,
        fetch_crash_reports,
        fetch_creator_clusters,
        fetch_dependency_candidates,
        fetch_duplicate_mesh_candidates,
        fetch_material_override_groups,
        fetch_partial_gzps_rows,
        fetch_schema_version,
        fetch_stats,
    )
except ModuleNotFoundError:
    from db import DEFAULT_DB_PATH, connect, resolve_db_path
    from diagnostics import (
        classify_candidate_strength,
        correlate_crash_report,
        fetch_crash_reports,
        fetch_creator_clusters,
        fetch_dependency_candidates,
        fetch_duplicate_mesh_candidates,
        fetch_material_override_groups,
        fetch_partial_gzps_rows,
        fetch_schema_version,
        fetch_stats,
    )


def build_plain_language_report(db_path: Path = DEFAULT_DB_PATH) -> str:
    connection = connect(resolve_db_path(db_path))
    stats = fetch_stats(connection)
    schema_version = fetch_schema_version(connection)
    dependency_candidates, unresolved_recolors, signatures = fetch_dependency_candidates(connection)
    duplicate_mesh = fetch_duplicate_mesh_candidates(connection)
    partial_gzps = fetch_partial_gzps_rows(connection)
    material_overrides = fetch_material_override_groups(connection)
    creator_clusters = fetch_creator_clusters(connection)
    crash_reports = fetch_crash_reports(connection)

    lines = []
    lines.append("Sims 2 Diagnostics Report")
    lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Schema version: {schema_version}")
    lines.append("")
    lines.append("Overview")
    lines.append(f"- Files scanned: {stats['file_count'] or 0}")
    lines.append(f"- Parsed packages: {stats['parsed_package_count'] or 0}")
    lines.append(f"- Indexed resources: {stats['resource_count'] or 0}")
    lines.append(f"- Crash logs imported: {stats['crash_count'] or 0}")
    lines.append(f"- Config logs imported: {stats['config_count'] or 0}")
    lines.append("")

    lines.append("Most Likely Current Issues")
    if duplicate_mesh:
        top = duplicate_mesh[0]
        lines.append(f"- Duplicate mesh package found: {top['file_name']} appears {top['copy_count']} times. This can make cleanup and testing harder.")
        for item in top["items"][:3]:
            lines.append(f"  path: {item['relative_path']}")
    else:
        lines.append("- No duplicate mesh-package groups were detected.")
    if unresolved_recolors:
        lines.append(f"- {len(unresolved_recolors)} recolor packages did not get a strong mesh match. Missing mesh dependencies are possible.")
        for profile in unresolved_recolors[:5]:
            lines.append(f"  unresolved: {profile['relative_path']}")
    else:
        lines.append("- No unresolved recolor packages were detected in the current scan.")
    if material_overrides:
        lines.append(f"- {len(material_overrides)} shared material or texture keys have divergent payloads. Visual overrides or graphics glitches are possible.")
    else:
        lines.append("- No divergent shared TXMT/TXTR override groups were detected.")
    if partial_gzps:
        lines.append(f"- {len(partial_gzps)} packages still have partial CAS metadata. This limits confidence for some dependency or catalog inferences.")
    else:
        lines.append("- CAS metadata is fully populated for the currently parsed packages.")
    lines.append("")

    lines.append("Crash Analysis")
    if crash_reports:
        for report in crash_reports:
            lines.append(f"- {report['file_name']} [{report['crash_category']}]")
            lines.append(f"  summary: {report['summary']}")
            if report["graphics_device"] or report["texture_memory_mb"] is not None:
                lines.append(f"  graphics: {report['graphics_device'] or '[unknown]'} | texture memory: {report['texture_memory_mb'] if report['texture_memory_mb'] is not None else '[unknown]'}")
            for note in correlate_crash_report(connection, report)[:3]:
                lines.append(f"  note: {note}")
    else:
        lines.append("- No crash or config logs have been imported yet.")
    lines.append("")

    lines.append("Dependency Highlights")
    shown = 0
    for recolor_id, candidates in dependency_candidates.items():
        if not candidates:
            continue
        top = candidates[0]
        if top["score"] < 12:
            continue
        file_row = connection.execute("SELECT relative_path FROM files WHERE id = ?", (recolor_id,)).fetchone()
        if not file_row:
            continue
        lines.append(f"- {file_row['relative_path']}")
        lines.append(f"  likely depends on: {top['mesh_path']} ({classify_candidate_strength(top['score']).lower()} score {top['score']})")
        shown += 1
        if shown >= 5:
            break
    if shown == 0:
        lines.append("- No strong dependency links were available to summarize.")
    lines.append("")

    lines.append("Simple Next Steps")
    if duplicate_mesh:
        lines.append("- Keep only one copy of exact duplicate mesh packages when you confirm the sets still work without the extra copies.")
    if crash_reports and any(report["crash_category"] == "graphics" for report in crash_reports):
        lines.append("- Check Graphics Rules, GPU recognition, and texture-memory settings before removing random custom content.")
    if unresolved_recolors:
        lines.append("- Test the unresolved recolor packages with their matching mesh folders restored or re-downloaded.")
    if partial_gzps:
        lines.append("- Treat packages with partial metadata as lower-confidence matches until they are tested in game.")
    lines.append("- When testing, move suspected content out in small batches instead of deleting everything at once.")
    lines.append("- Re-import new crash logs after each test round so the crash history stays attached to the content scan.")
    if creator_clusters:
        lines.append("- Keep creator-based sets together while testing; splitting them apart can create false missing-mesh results.")

    connection.close()
    return "\n".join(lines) + "\n"


def write_report(report_dir: Path, db_path: Path = DEFAULT_DB_PATH) -> Path:
    report_dir = report_dir.expanduser().resolve()
    report_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    report_path = report_dir / f"sims2-diagnostics-report-{timestamp}.txt"
    report_path.write_text(build_plain_language_report(db_path), encoding="utf-8")
    return report_path
