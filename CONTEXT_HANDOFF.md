# Sims 2 CC Diagnostics Handoff

## Current Purpose

Local diagnostics platform for The Sims 2 custom content.

Current capabilities:

- recursive file inventory
- exact duplicate detection
- DBPF package parsing
- resource indexing and body hashing
- resource-specific parsing for `OBJD`, `BHAV`, and `TTAB`
- package classification
- manual conflict review
- dependency grouping for CAS-style mesh/recolor sets
- orphan recolor detection
- duplicate mesh-package detection
- CAS shared-asset classification
- explicit `3IDR`-based recolor-to-mesh dependency matching
- structured `GZPS` metadata persistence
- package detail pages and compare view
- creator clustering from parsed CAS metadata
- content-health diagnostics route
- crash/config-log ingestion and correlation
- desktop launcher with saved folder settings and text-report output
- Windows `.exe` build scaffolding

## Important Files

- `app/schema.sql`
- `app/bootstrap_db.py`
- `app/scan_content.py`
- `app/dbpf_parser.py`
- `app/resource_parsers.py`
- `app/web_ui.py`
- `app/crash_parser.py`
- `app/import_crash_logs.py`
- `app/desktop_app.py`
- `app/reporting.py`
- `app/settings_store.py`
- `Sims2CCDiagnostics.spec`
- `build_windows_exe.bat`
- `README.md`

## Current Data Findings

Most recent validated scan:

```text
python3 Sims2-CC-Diagnostics/app/scan_content.py --root Sims2-Test-Packages
```

Result:

- `40` files cataloged
- `35` parsed `.package` files
- `1` exact duplicate group
- `248` indexed resources

Package classification for `Sims2-Test-Packages`:

- `30` `Recolor or Property Set`
- `5` `Mesh Package`

This folder currently looks like CAS/bodyshop-style content, not object or hack content.

Observed resource mix:

- `TXTR`
- `TXMT`
- `3IDR`
- `GZPS`
- `XOBJ`
- `STR#`
- `GMDC`
- `GMND`
- `CRES`
- `SHPE`

Counts for `OBJD`, `BHAV`, and `TTAB` in this real dataset are currently `0`.

## UI Routes

- `/` inventory and classification
- `/conflicts` resource-level conflict views
- `/dependencies` folder-local mesh/recolor bundle grouping plus orphan-recolor, duplicate-mesh, and CAS shared-asset diagnostics
- `/reviews` manual pair-review ledger
- `/package/{id}` package detail view
- `/compare?left=<id>&right=<id>` side-by-side compare view
- `/health` content-health dashboard
- `/crashes` crash/config correlation dashboard

Desktop path:

- `app/desktop_app.py` launches a Tkinter desktop app
- persists folder settings in a JSON settings file
- initializes DB, scans content, imports logs, and writes a plain-language report

## Parser State

### DBPF

`app/dbpf_parser.py` currently supports:

- DBPF major `1`
- index major `7`
- index minor `0`, `1`, and `2`

`index_minor == 2` support mattered for the real test packages.

### Resource-Specific Parsers

`app/resource_parsers.py` currently includes:

- `OBJD` parser
- `BHAV` metadata parser
- `TTAB` metadata parser
- `3IDR` parser
- lightweight `TXMT` parser
- first-pass `GZPS` parser

These are useful for object/hack content, but the current real folder is CAS-focused, so the next parser effort should target scenegraph/CAS dependencies instead.

## Classification Layer

Current package categories:

- `Object or Hack Package`
- `Behavior Mod`
- `CAS Hybrid Set`
- `Mesh Package`
- `Recolor or Property Set`
- `Text Support Package`
- `Unclassified Package`

The current real test set validates the `Mesh Package` and `Recolor or Property Set` paths.

## Dependency Layer

`/dependencies` groups packages by folder and classifies likely bundles such as:

- one mesh package plus several recolors in the same folder
- recolor-only folders
- hybrid or mixed folders

It now also adds:

- orphan recolor folder detection
- duplicate mesh package detection across folders
- CAS shared-asset classification to separate likely harmless duplicate distribution from stronger collision candidates
- explicit `3IDR` reference resolution so recolor packages can point directly at mesh-side `CRES` and `SHPE` resources in other packages

This is still intentionally heuristic, but it is already useful for the current test set.

Current observed outputs for `Sims2-Test-Packages`:

- `5` likely mesh bundles
- `0` orphan recolor folders
- `1` duplicate mesh-package group
- `4` shared CAS asset groups, all currently low-risk duplicate mesh assets
- recolors now resolve to their mesh packages with strong scores backed by `3IDR` references
- `30` persisted `GZPS` rows
- `1` repeated creator cluster in current parsed metadata
- improved `GZPS` fuzzy extraction removed blank `name` and `creator` values, but many `family` fields remain blank on packed records
- crash importer now stores crash and config logs in `crash_reports`
- plain-language reports can now be written without using the browser UI

## Important Bug Already Fixed

Runtime SQLite connections originally did not enable foreign-key enforcement. That caused stale child rows to survive rescans and made resource counts appear doubled.

Fixes applied:

- `PRAGMA foreign_keys = ON` added to runtime connections
- explicit root-level cleanup added in `app/scan_content.py`

Validated after fix:

- real-folder rescans return `248` indexed resources, not `496`

## Recommended Next Steps

The next high-value work is CAS dependency analysis, not more object/hack parsing.

Best next implementation order:

1. improve `GZPS` field extraction quality for compressed/variant records that currently come back partially blank
2. parse or infer when `TXMT`/`TXTR`/`GZPS` sets point at missing scenegraph anchors
3. add default-replacement and divergent mesh-variant diagnostics
4. enrich compare/detail pages with manual notes and cleanup actions
5. improve crash-log parsing against real Sims 2 crash dumps and config-log formats instead of synthetic fixtures
6. separate harmless shared assets from risky package collisions with more type-specific logic

## Validation Commands

Compile:

```bash
python3 -m py_compile \
  Sims2-CC-Diagnostics/app/bootstrap_db.py \
  Sims2-CC-Diagnostics/app/scan_content.py \
  Sims2-CC-Diagnostics/app/dbpf_parser.py \
  Sims2-CC-Diagnostics/app/resource_parsers.py \
  Sims2-CC-Diagnostics/app/web_ui.py
```

Scan real folder:

```bash
python3 Sims2-CC-Diagnostics/app/scan_content.py --root Sims2-Test-Packages
```

Run UI:

```bash
uvicorn app.web_ui:app --app-dir Sims2-CC-Diagnostics --reload
```

## Notes On Context

If the chat context needs to be reset, this file should be enough to resume without reconstructing the project history from scratch.
