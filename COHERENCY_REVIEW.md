# Coherency Review Against README Purpose

Date: 2026-03-27

## Verdict

The codebase is **largely coherent** with the README's primary purpose: a local Sims 2 custom-content diagnostics platform for inventory, duplicate detection, DBPF indexing, conflict surfacing, crash-log correlation, and local UI workflows.

## Strong Alignment

1. **Core scan + inventory pipeline exists and is implemented.**
   - Recursive file scanning, hashing, package detection, DBPF parsing, and persistence are implemented in `app/scan_content.py`.
2. **SQLite-backed diagnostics model is present.**
   - Schema and bootstrap path are present (`app/schema.sql`, `app/bootstrap_db.py`).
3. **Desktop-first workflow exists.**
   - Tkinter launcher supports DB init, content scan, crash-log import, and report generation (`app/desktop_app.py`).
4. **Optional browser UI exists with the documented routes.**
   - FastAPI app provides inventory, conflicts, dependencies, reviews, health, package detail, compare, and crashes pages (`app/web_ui.py`).
5. **Crash/config log ingestion and analysis are implemented.**
   - Parsing and import logic present (`app/crash_parser.py`, `app/import_crash_logs.py`) and exposed in UI/reporting.
6. **Windows packaging scaffolding is present.**
   - The referenced `Sims2CCDiagnostics.spec` and build batch script are both present in the repository root.

## Coherency Gaps (README vs repo reality)

1. **Quick Start uses machine-specific absolute paths.**
   - README includes `/home/austin/...` examples, which conflicts with portability implied by the project purpose.
2. **Resource-parser progress wording is broader than persisted-scan behavior.**
   - README progress implies broad parser completion, but scan persistence currently writes structured rows for `OBJD`, `BHAV`, `TTAB`, and `GZPS`; `3IDR`/`TXMT` are used during dependency analysis in the web layer rather than persisted as first-class tables during scan.
3. **“Next High-Value Steps” includes a step that appears already complete.**
   - README lists “Parse package internals into `package_resources`” as next, but the current scanner already inserts `package_resources` records for parsed packages.

## Extensibility Assessment

Extensibility is good and consistent with stated goals:

- Parsing is modular (`app/resource_parsers.py`) and can be expanded by type.
- Classification and dependency logic are isolated in UI/service functions (`app/web_ui.py`), enabling refinement without redesigning storage.
- Schema already anticipates richer diagnostics and review workflows (`pair_reviews`, resource-level keys/hashes, crash tables).

## Recommended Next Coherency Fixes

1. Replace absolute-path Quick Start examples with repository-relative examples.
2. Clarify in README that `3IDR`/`TXMT` are currently leveraged primarily in dependency inference (on-demand) unless/until persisted extraction tables are added.
3. Refresh “Next High-Value Steps” so it reflects work not already implemented.

