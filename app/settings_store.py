#!/usr/bin/env python3

from __future__ import annotations

import json
import os
from pathlib import Path


APP_NAME = "Sims2CCDiagnostics"


def default_settings_path() -> Path:
    appdata = os.environ.get("APPDATA")
    if appdata:
        return Path(appdata) / APP_NAME / "settings.json"
    return Path.home() / ".config" / APP_NAME / "settings.json"


def load_settings(path: Path | None = None) -> dict[str, str]:
    settings_path = (path or default_settings_path()).expanduser().resolve()
    if not settings_path.exists():
        return {
            "content_root": "",
            "logs_root": "",
            "reports_dir": str(settings_path.parent / "reports"),
            "db_path": str(settings_path.parent / "sims2_cc.db"),
            "last_report_path": "",
        }
    data = json.loads(settings_path.read_text(encoding="utf-8"))
    defaults = {
        "content_root": "",
        "logs_root": "",
        "reports_dir": str(settings_path.parent / "reports"),
        "db_path": str(settings_path.parent / "sims2_cc.db"),
        "last_report_path": "",
    }
    defaults.update({key: str(value) for key, value in data.items()})
    return defaults


def save_settings(settings: dict[str, str], path: Path | None = None) -> Path:
    settings_path = (path or default_settings_path()).expanduser().resolve()
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(json.dumps(settings, indent=2, sort_keys=True), encoding="utf-8")
    return settings_path
