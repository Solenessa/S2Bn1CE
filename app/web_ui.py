#!/usr/bin/env python3

from __future__ import annotations

import html
import sqlite3
from pathlib import Path
from urllib.parse import urlencode

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse

try:
    from app.db import connect as db_connect
    from app.db import resolve_web_db_path
    import app.diagnostics as diag
except ModuleNotFoundError:
    from db import connect as db_connect
    from db import resolve_web_db_path
    import diagnostics as diag


APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent


def create_app(db_path: Path | None = None) -> FastAPI:
    fastapi_app = FastAPI(title="Sims 2 CC Diagnostics")
    fastapi_app.state.db_path = resolve_web_db_path(db_path)
    return fastapi_app


app = create_app()


def get_connection() -> sqlite3.Connection:
    return db_connect(app.state.db_path)

def save_review(left_file_id: int, right_file_id: int, status: str) -> None:
    connection = get_connection()
    diag.save_review(connection, left_file_id, right_file_id, status)
    connection.close()


def clear_review(left_file_id: int, right_file_id: int) -> None:
    connection = get_connection()
    diag.clear_review(connection, left_file_id, right_file_id)
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

pair_key = diag.pair_key
fetch_review_map = diag.fetch_review_map
fetch_stats = diag.fetch_stats
fetch_recent_files = diag.fetch_recent_files
fetch_schema_version = diag.fetch_schema_version
fetch_duplicate_groups = diag.fetch_duplicate_groups
fetch_duplicate_files = diag.fetch_duplicate_files
fetch_resource_conflict_groups = diag.fetch_resource_conflict_groups
fetch_resource_conflict_files = diag.fetch_resource_conflict_files
fetch_resource_type_breakdown = diag.fetch_resource_type_breakdown
fetch_objd_guid_conflicts = diag.fetch_objd_guid_conflicts
fetch_objd_conflict_files = diag.fetch_objd_conflict_files
fetch_bhav_heavy_files = diag.fetch_bhav_heavy_files
fetch_ttab_heavy_files = diag.fetch_ttab_heavy_files
fetch_ttab_links = diag.fetch_ttab_links
fetch_package_profiles = diag.fetch_package_profiles
fetch_category_breakdown = diag.fetch_category_breakdown
fetch_package_metadata_map = diag.fetch_package_metadata_map
fetch_creator_clusters = diag.fetch_creator_clusters
fetch_package_detail = diag.fetch_package_detail
fetch_dependency_groups = diag.fetch_dependency_groups
fetch_orphan_recolor_groups = diag.fetch_orphan_recolor_groups
fetch_duplicate_mesh_candidates = diag.fetch_duplicate_mesh_candidates
fetch_cas_shared_asset_groups = diag.fetch_cas_shared_asset_groups
fetch_dependency_candidates = diag.fetch_dependency_candidates
fetch_file_row = diag.fetch_file_row
fetch_material_override_groups = diag.fetch_material_override_groups
fetch_partial_gzps_rows = diag.fetch_partial_gzps_rows
fetch_crash_reports = diag.fetch_crash_reports
correlate_crash_report = diag.correlate_crash_report
classify_resource_conflict = diag.classify_resource_conflict
classify_candidate_strength = diag.classify_candidate_strength

@app.get("/", response_class=HTMLResponse)
def home(q: str = "", category: str = "") -> HTMLResponse:
    connection = get_connection()
    stats = fetch_stats(connection)
    schema_version = fetch_schema_version(connection)
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
              <div class="stat"><span class="subtle">Schema</span><strong>v{schema_version}</strong></div>
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
