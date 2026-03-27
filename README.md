# Sims 2 CC Diagnostics

Local platform for inventorying, measuring, and diagnosing The Sims 2 custom content and mod conflicts.

## Runtime Stack

The project currently uses:

- Python
- SQLite
- Tkinter for the desktop launcher
- FastAPI for the optional browser UI
- PyInstaller for Windows executable packaging

## Current Scope

This first scaffold focuses on the part we can solve immediately:

- scan a Downloads or mods folder recursively
- catalog `.package`, `.sims2pack`, and common support files
- hash files and detect exact duplicates
- parse DBPF package indexes from `.package` files
- store per-resource keys, types, and offsets
- hash individual resource payloads
- surface shared resource-key conflicts across multiple packages
- store package metadata in SQLite
- expose a local web UI for inventory, duplicate review, conflict review, and dependency grouping

This currently parses package **indexes**, not full resource bodies. That means it can already detect many strong conflict signals based on shared resource keys, but it does **not** yet inspect BHAV bytecode, TTAB contents, GUID semantics, or creator-specific metadata inside resource bodies.

It does, however, hash each indexed resource payload, which lets the app distinguish:

- same key, same payload: often duplicate or redistributed content
- same key, different payload: much stronger override or conflict candidate

## Resource-Specific Parsing Progress

Completed:

- `OBJD` parser
  - extracts object name
  - extracts object GUID and original GUID
  - extracts price, object type, slot ID, interaction table ID, expansion flag
  - surfaces cross-package GUID collisions in the conflict UI
- `BHAV` metadata parser
  - extracts function name
  - extracts signature, instruction count, argument count, local variable count
  - extracts first and last opcode values
  - surfaces behavior-heavy packages in the inventory UI
- `TTAB` metadata and linkage parser
  - extracts TTAB format code
  - stores TTAB instance ids
  - links TTAB instance ids back to `OBJD.interaction_table_id`
  - surfaces object-to-interaction-table links in the conflict UI
- `3IDR` parser
  - parses explicit resource-reference entries
  - resolves cross-package references to mesh-side resources such as `CRES` and `SHPE`
  - promotes recolor-to-mesh matching from folder heuristics to real package-reference evidence
  - currently used for dependency inference in the UI/reporting layer (not yet persisted to a dedicated extraction table during scan)
- `TXMT` lightweight parser
  - extracts material/resource-name strings
  - exposes material identity hints for dependency display
  - currently used for dependency inference/display in the UI/reporting layer (not yet persisted to a dedicated extraction table during scan)
- `GZPS` structured parser
  - persists first-pass CAS metadata such as item name, creator, family, and type
  - supports creator clustering and per-package CAS detail views
  - now uses fuzzy fallback extraction for packed records, which eliminated blank `name` and `creator` fields on the current test set

Current classification layer:

- `Object or Hack Package`
- `Behavior Mod`
- `CAS Hybrid Set`
- `Mesh Package`
- `Recolor or Property Set`
- `Text Support Package`
- `Unclassified Package`

Current dependency layer:

- folder-local grouping of likely mesh and recolor bundles
- bundle detection for mesh-plus-recolor sets
- a dedicated `/dependencies` page for CAS-style content sets
- orphan recolor detection for folders with no local mesh or hybrid anchor
- duplicate mesh-package detection across folders
- CAS shared-asset classification that separates low-risk duplicate distribution from stronger collision signals
- explicit `3IDR`-based recolor-to-mesh linkage where the package directly references external mesh resources

Planned next:

- scenegraph and CAS dependency analysis across `3IDR`, `CRES`, `SHPE`, `GMND`, `GMDC`, `TXMT`, `TXTR`, and `GZPS`
- deeper parsing of scenegraph and CAS resources instead of folder-level inference alone
- higher-confidence conflict classification using parsed object and behavior data
- deeper TTAB row parsing when a reliable field layout source is pinned down

## Project Layout

- `app/schema.sql`: SQLite schema
- `app/bootstrap_db.py`: creates or rebuilds the SQLite database
- `app/scan_content.py`: scans a Sims 2 content folder into the database
- `app/import_crash_logs.py`: imports Sims 2 crash logs and config logs
- `app/desktop_app.py`: desktop launcher for scans, log imports, and report generation
- `app/reporting.py`: plain-language report generation
- `app/settings_store.py`: persisted desktop-app folder settings
- `app/web_ui.py`: FastAPI browser UI
- `data/sims2_cc.db`: generated database

## Quick Start

Create the database:

```bash
python3 app/bootstrap_db.py
```

Scan a content folder:

```bash
python3 app/scan_content.py --root "/path/to/The Sims 2/Downloads"
python3 app/import_crash_logs.py --root "/path/to/The Sims 2/Logs"
```

Launch the UI:

```bash
uvicorn app.web_ui:app --reload
```

Then open `http://127.0.0.1:8000`.

Desktop launcher:

```bash
python3 app/desktop_app.py
```

The desktop app is the path intended for Windows portability. It stores folder settings, initializes the database, scans custom content, imports logs, and writes a plain-language text report without requiring the user to run a server manually.

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

## Next High-Value Steps

1. Persist dedicated structured extraction tables for scenegraph-focused parsers (starting with `3IDR` and `TXMT`) to complement existing dependency inference
2. Deepen scenegraph and CAS dependency analysis across `3IDR`, `CRES`, `SHPE`, `GMND`, `GMDC`, `TXMT`, `TXTR`, and `GZPS`
3. Distinguish harmless duplicates from dangerous behavior conflicts with higher-confidence scoring
4. Add manual pair review notes and confirmed conflict records
5. Add “safe to test remove” and “never remove” labels
