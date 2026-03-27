# Sims 2 CC Diagnostics

Local tool for inventorying and reviewing The Sims 2 custom content. It can surface duplicates, parsed package metadata, and heuristic conflict or dependency candidates, but it is not yet a validated authoritative conflict detector.

## Runtime Stack

The project currently uses:

- Python
- SQLite
- Tkinter for the desktop launcher
- FastAPI for the optional browser UI
- PyInstaller for Windows executable packaging

## Current Scope

Current focus:

- scan a Downloads or mods folder recursively
- catalog `.package`, `.sims2pack`, and common support files
- hash files and detect exact duplicates
- parse DBPF package indexes from `.package` files
- store per-resource keys, types, and offsets
- hash individual resource payloads
- surface shared resource-key conflicts across multiple packages
- store package metadata in SQLite
- expose a local web UI for inventory, duplicate review, conflict review, and dependency grouping

This currently parses package indexes plus a limited set of resource-specific metadata. The conflict and dependency views are still heuristic. They are useful for narrowing manual review, not for proving that content is safe or unsafe by themselves.

It does hash indexed resource payloads, which helps distinguish:

- same key, same payload: often duplicate or redistributed content
- same key, different payload: much stronger override or conflict candidate

## Resource-Specific Parsing Progress

Implemented parser slices:

- `OBJD` parser
  - extracts object name
  - extracts object GUID and original GUID
  - extracts price, object type, slot ID, interaction table ID, expansion flag
  - surfaces cross-package GUID collisions for review
- `BHAV` metadata parser
  - extracts function name
  - extracts signature, instruction count, argument count, local variable count
  - extracts first and last opcode values
  - surfaces behavior-heavy packages for triage
- `TTAB` metadata and linkage parser
  - extracts TTAB format code
  - stores TTAB instance ids
  - links TTAB instance ids back to `OBJD.interaction_table_id`
  - surfaces object-to-interaction-table links
- `3IDR` parser
  - parses explicit resource-reference entries
  - stores explicit cross-package references to mesh-side resources such as `CRES` and `SHPE`
- `TXMT` lightweight parser
  - extracts material/resource-name strings
  - stores material-name hints for dependency display
- `GZPS` structured parser
  - persists first-pass CAS metadata such as item name, creator, family, and type
  - supports creator clustering and per-package CAS detail views
  - uses fuzzy fallback extraction for packed records on the current sample set

Current classification layer:

- `Object or Hack Package`
- `Behavior Mod`
- `CAS Hybrid Set`
- `Mesh Package`
- `Recolor or Property Set`
- `Text Support Package`
- `Unclassified Package`

Current dependency heuristics:

- folder-local grouping of likely mesh and recolor bundles
- bundle detection for mesh-plus-recolor sets
- a dedicated `/dependencies` page for CAS-style content sets
- orphan recolor detection for folders with no local mesh or hybrid anchor
- duplicate mesh-package detection across folders
- CAS shared-asset classification that separates low-risk duplicate distribution from stronger collision signals
- explicit `3IDR`-based recolor-to-mesh linkage where the package directly references external mesh resources
- persisted scenegraph-name hints extracted during scan so the UI and reporting paths do not reparse package files

Still missing or weak:

- scenegraph and CAS dependency analysis across `3IDR`, `CRES`, `SHPE`, `GMND`, `GMDC`, `TXMT`, `TXTR`, and `GZPS`
- deeper parsing of scenegraph and CAS resources instead of folder-level inference alone
- higher-confidence conflict classification using validated object and behavior data
- deeper TTAB row parsing when a reliable field layout source is pinned down

## Project Layout

- `app/schema.sql`: SQLite schema
- `app/bootstrap_db.py`: initializes the SQLite database and provides a guarded reset flow
- `app/db.py`: shared database path, connection, and backup helpers
- `app/migrations.py`: schema-versioned SQLite migrations
- `app/diagnostics.py`: shared diagnostics queries and heuristics used by the UI and report generator
- `app/scan_content.py`: scans a Sims 2 content folder into the database
- `app/import_crash_logs.py`: imports Sims 2 crash logs and config logs
- `app/desktop_app.py`: desktop launcher for scans, log imports, and report generation
- `app/reporting.py`: plain-language report generation
- `app/settings_store.py`: persisted desktop-app folder settings
- `app/web_ui.py`: FastAPI browser UI
- `data/sims2_cc.db`: generated database

## Quick Start

Create or update the database schema:

```bash
python3 app/bootstrap_db.py
```

If you intentionally want to recreate the database file, use the guarded reset flow:

```bash
python3 app/bootstrap_db.py --db "/path/to/sims2_cc.db" --reset --yes
```

That reset path creates a timestamped backup before recreating the database.
Normal initialization also runs schema-versioned migrations in place for existing databases.

Scan a content folder:

```bash
python3 app/scan_content.py --root "/path/to/The Sims 2/Downloads"
python3 app/import_crash_logs.py --root "/path/to/The Sims 2/Logs"
```

Launch the UI:

```bash
uvicorn app.web_ui:app --reload
```

To point the browser UI at a specific database file, set `SIMS2_CC_DB_PATH` before starting Uvicorn:

```bash
SIMS2_CC_DB_PATH="/path/to/sims2_cc.db" uvicorn app.web_ui:app --reload
```

Then open `http://127.0.0.1:8000`.

Desktop launcher:

```bash
python3 app/desktop_app.py
```

The desktop app is the path intended for Windows portability. It stores folder settings, initializes the database, scans custom content, imports logs, and writes a plain-language text report without requiring the user to run a server manually. The reset action now requires confirmation and creates a backup first.

Current routes:

- `/`: inventory and package classification
- `/conflicts`: resource-key, GUID, and interaction-table conflict views
- `/dependencies`: mesh/recolor bundle grouping
- `/reviews`: manual review ledger
- `/health`: content-health diagnostics
- `/package/{id}`: per-package detail view
- `/compare?left=<id>&right=<id>`: side-by-side compare view
- `/crashes`: crash/config-log correlation view

## Windows EXE Build

The project includes Windows packaging scaffolding:

- `requirements.txt`
- `Sims2CCDiagnostics.spec`
- `build_windows_exe.bat`

Build flow on Windows:

```bat
build_windows_exe.bat
```

Expected output:

```text
dist\Sims2CCDiagnostics.exe
```

The desktop launcher is the executable target. The browser UI remains optional.

## Current Data Model

The database already tracks:

- scan runs
- scanned files
- normalized file types
- file hashes
- duplicate groups
- parsed DBPF header and index versions
- package resource keys and resource type labels
- persisted scenegraph-name hints and explicit `3IDR` resource links used by dependency heuristics
- parsed `GZPS` metadata
- imported crash and config logs
- user review state for duplicate and conflict candidates

It is designed to expand into:

- parsed package resources
- resource-level override conflicts
- default replacement detection
- behavior-mod collision detection
- missing dependency tracking
- creator and set grouping
- neighborhood-safe vs global-hack classification

## Testing

The repo includes fixture-driven `unittest` coverage for:

- DBPF happy-path parsing
- malformed DBPF rejection
- scan-time persistence of dependency metadata
- non-destructive default database initialization
- schema version migration behavior
- migration backfill of duplicate-group state for older databases
- web DB path selection via environment or explicit app creation

Run the tests with:

```bash
python3 -m unittest discover -s tests
```

## Next High-Value Steps

1. Validate parser offsets and heuristics against a broader real-world package corpus
2. Expand migrations beyond schema-additive changes and cover real data backfills when parser output evolves
3. Distinguish harmless duplicates from dangerous behavior conflicts with better resource semantics
4. Add “safe to test remove” and “never remove” labels
5. Expand fixture coverage for known Sims 2 package variants and edge cases
