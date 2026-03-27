#!/usr/bin/env python3

from __future__ import annotations

import os
from pathlib import Path
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

try:
    from app.db import backup_database, initialize_database
    from app.import_crash_logs import import_logs
    from app.reporting import write_report
    from app.scan_content import scan_root
    from app.settings_store import load_settings, save_settings
except ModuleNotFoundError:
    from db import backup_database, initialize_database
    from import_crash_logs import import_logs
    from reporting import write_report
    from scan_content import scan_root
    from settings_store import load_settings, save_settings


APP_TITLE = "Sims 2 CC Diagnostics"


class DesktopApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("880x620")
        self.settings = load_settings()

        self.content_root = tk.StringVar(value=self.settings["content_root"])
        self.logs_root = tk.StringVar(value=self.settings["logs_root"])
        self.reports_dir = tk.StringVar(value=self.settings["reports_dir"])
        self.db_path = tk.StringVar(value=self.settings["db_path"])
        self.last_report_path = tk.StringVar(value=self.settings["last_report_path"])
        self.status = tk.StringVar(value="Ready.")

        self._build()

    def _build(self) -> None:
        frame = ttk.Frame(self.root, padding=16)
        frame.pack(fill="both", expand=True)

        title = ttk.Label(frame, text=APP_TITLE, font=("Segoe UI", 18, "bold"))
        title.pack(anchor="w")
        ttk.Label(
            frame,
            text="Portable desktop launcher for scanning Sims 2 custom content, importing crash logs, and writing plain-language reports.",
            wraplength=760,
        ).pack(anchor="w", pady=(4, 12))

        form = ttk.Frame(frame)
        form.pack(fill="x")
        self._path_row(form, "Custom Content Folder", self.content_root, 0)
        self._path_row(form, "Crash / Config Logs", self.logs_root, 1)
        self._path_row(form, "Reports Folder", self.reports_dir, 2)
        self._path_row(form, "Database File", self.db_path, 3, directory=False)

        actions = ttk.Frame(frame)
        actions.pack(fill="x", pady=(16, 12))
        ttk.Button(actions, text="Save Settings", command=self.save_current_settings).pack(side="left", padx=(0, 8))
        ttk.Button(actions, text="Initialize Database", command=lambda: self.run_background(self.initialize_db)).pack(side="left", padx=8)
        ttk.Button(actions, text="Reset Database", command=self.reset_db).pack(side="left", padx=8)
        ttk.Button(actions, text="Scan Custom Content", command=lambda: self.run_background(self.scan_content)).pack(side="left", padx=8)
        ttk.Button(actions, text="Import Crash Logs", command=lambda: self.run_background(self.import_logs)).pack(side="left", padx=8)
        ttk.Button(actions, text="Generate Report", command=lambda: self.run_background(self.generate_report)).pack(side="left", padx=8)
        ttk.Button(actions, text="Open Last Report", command=self.open_last_report).pack(side="left", padx=8)

        output = ttk.LabelFrame(frame, text="Status", padding=12)
        output.pack(fill="both", expand=True)
        ttk.Label(output, textvariable=self.status, wraplength=780, justify="left").pack(anchor="w")

    def _path_row(self, parent: ttk.Frame, label: str, variable: tk.StringVar, row: int, directory: bool = True) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=(0, 12), pady=8)
        entry = ttk.Entry(parent, textvariable=variable, width=84)
        entry.grid(row=row, column=1, sticky="ew", pady=8)
        parent.grid_columnconfigure(1, weight=1)
        button = ttk.Button(parent, text="Browse", command=lambda: self.browse_path(variable, directory=directory))
        button.grid(row=row, column=2, padx=(8, 0), pady=8)

    def browse_path(self, variable: tk.StringVar, directory: bool = True) -> None:
        current = variable.get().strip()
        initial = current if current else str(Path.home())
        if directory:
            selected = filedialog.askdirectory(initialdir=initial)
        else:
            selected = filedialog.asksaveasfilename(
                initialdir=str(Path(initial).parent if current else Path.home()),
                initialfile=Path(current).name if current else "sims2_cc.db",
                defaultextension=".db",
                filetypes=[("SQLite DB", "*.db"), ("All Files", "*.*")],
            )
        if selected:
            variable.set(selected)

    def save_current_settings(self) -> None:
        self.settings.update(
            {
                "content_root": self.content_root.get().strip(),
                "logs_root": self.logs_root.get().strip(),
                "reports_dir": self.reports_dir.get().strip(),
                "db_path": self.db_path.get().strip(),
                "last_report_path": self.last_report_path.get().strip(),
            }
        )
        settings_path = save_settings(self.settings)
        self.status.set(f"Settings saved to {settings_path}")

    def run_background(self, fn) -> None:
        self.save_current_settings()
        thread = threading.Thread(target=self._run_safe, args=(fn,), daemon=True)
        thread.start()

    def _run_safe(self, fn) -> None:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            self.root.after(0, lambda: messagebox.showerror(APP_TITLE, str(exc)))
            self.root.after(0, lambda: self.status.set(f"Error: {exc}"))

    def initialize_db(self) -> None:
        db_path = initialize_database(Path(self.db_path.get()))
        self.root.after(0, lambda: self.status.set(f"Initialized database at {db_path}"))

    def reset_db(self) -> None:
        db_path = Path(self.db_path.get())
        if not messagebox.askyesno(
            APP_TITLE,
            f"Reset the database at\n{db_path}\n\nThis creates a backup first and then recreates the database file.",
        ):
            return
        self.run_background(self._reset_db_background)

    def _reset_db_background(self) -> None:
        db_path = Path(self.db_path.get())
        backup_path = ""
        if db_path.exists():
            backup_path = str(backup_database(db_path))
            db_path.unlink()
        initialized = initialize_database(db_path)
        if backup_path:
            self.root.after(0, lambda: self.status.set(f"Database reset. Backup={backup_path} New={initialized}"))
            return
        self.root.after(0, lambda: self.status.set(f"Database created at {initialized}"))

    def scan_content(self) -> None:
        result = scan_root(Path(self.content_root.get()), Path(self.db_path.get()))
        self.root.after(
            0,
            lambda: self.status.set(
                f"Content scan complete. Files={result['files_cataloged']} Parsed={result['parsed_packages']} Resources={result['indexed_resources']}"
            ),
        )

    def import_logs(self) -> None:
        result = import_logs(Path(self.logs_root.get()), Path(self.db_path.get()))
        self.root.after(
            0,
            lambda: self.status.set(
                f"Imported {result['imported']} log files. Crashes={result['crash_count']} Configs={result['config_count']}"
            ),
        )

    def generate_report(self) -> None:
        report_path = write_report(Path(self.reports_dir.get()), Path(self.db_path.get()))
        self.last_report_path.set(str(report_path))
        self.save_current_settings()
        self.root.after(0, lambda: self.status.set(f"Report written to {report_path}"))

    def open_last_report(self) -> None:
        report_path = Path(self.last_report_path.get().strip())
        if not report_path.exists():
            messagebox.showinfo(APP_TITLE, "No report file is available yet.")
            return
        if os.name == "nt":
            os.startfile(report_path)  # type: ignore[attr-defined]  # nosec B606
            return
        messagebox.showinfo(APP_TITLE, f"Last report: {report_path}")


def main() -> None:
    root = tk.Tk()
    ttk.Style().theme_use("clam")
    DesktopApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
