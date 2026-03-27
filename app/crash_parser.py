#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from pathlib import Path


KEY_VALUE_RE = re.compile(r"^\s*([^:=]+?)\s*[:=]\s*(.*?)\s*$")
HEX_RE = re.compile(r"0x[0-9A-Fa-f]+")


@dataclass
class ParsedCrashReport:
    source_path: str
    file_name: str
    log_type: str
    sha256: str
    occurred_at_text: str
    app_name: str
    exception_code: str
    exception_module: str
    fault_address: str
    crash_category: str
    summary: str
    graphics_device: str
    graphics_vendor: str
    driver_version: str
    texture_memory_mb: int | None
    os_version: str
    memory_hint: str
    raw_text: str


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def read_text(path: Path) -> str:
    return path.read_text(encoding="latin-1", errors="ignore")


def extract_key_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        match = KEY_VALUE_RE.match(line)
        if not match:
            continue
        key = match.group(1).strip().lower()
        value = match.group(2).strip()
        if key and value and key not in values:
            values[key] = value
    return values


def detect_log_type(path: Path, text: str) -> str:
    lowered_name = path.name.lower()
    lowered_text = text.lower()
    if "config-log" in lowered_name or "config-log" in lowered_text:
        return "config"
    return "crash"


def parse_texture_memory(text: str, values: dict[str, str]) -> int | None:
    for key in ["texture memory", "texturememory", "texture memory mb"]:
        if key in values:
            match = re.search(r"\d+", values[key])
            if match:
                return int(match.group(0))
    match = re.search(r"texture memory[^0-9]*(\d+)", text, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def parse_graphics_field(values: dict[str, str], *keys: str) -> str:
    for key in keys:
        if key in values:
            return values[key]
    return ""


def parse_exception_module(text: str, values: dict[str, str]) -> str:
    for key in ["exception module", "fault module name", "module"]:
        if key in values:
            return values[key]
    match = re.search(r"exception module[^A-Za-z0-9_.-]*([A-Za-z0-9_.-]+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return ""


def parse_exception_code(text: str, values: dict[str, str]) -> str:
    for key in ["exception code", "exception"]:
        if key in values:
            return values[key]
    match = re.search(r"exception code[^0-9A-Fa-fx]*(0x[0-9A-Fa-f]+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return ""


def parse_fault_address(text: str, values: dict[str, str]) -> str:
    for key in ["fault address", "address", "exception address"]:
        if key in values:
            return values[key]
    match = re.search(r"(fault|exception) address[^0-9A-Fa-fx]*(0x[0-9A-Fa-f]+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(2)
    return ""


def classify_crash(log_type: str, text: str, values: dict[str, str], exception_code: str, exception_module: str, texture_memory_mb: int | None) -> tuple[str, str, str]:
    lowered = text.lower()
    module = exception_module.lower()
    if log_type == "config":
        summary = "Configuration log imported. This is useful for graphics and compatibility correlation, not as a crash by itself."
        return "config", summary, ""
    if any(term in lowered for term in ["direct3d", "graphics rules", "graphic device", "texture memory"]) or module in {"d3d9.dll", "atioglxx.dll", "nvoglnt.dll", "ig4icd32.dll"}:
        summary = "Crash log shows graphics- or driver-adjacent signals. Check graphics rules, texture memory, GPU support, and large texture-heavy content."
        return "graphics", summary, f"texture memory={texture_memory_mb}" if texture_memory_mb is not None else ""
    if exception_code.lower() in {"0xc0000005", "c0000005"} or "access violation" in lowered:
        summary = "Crash log looks like an access violation. In Sims 2 this can be caused by hack conflicts, bad object state, driver instability, or memory pressure."
        return "memory", summary, exception_module
    if any(term in lowered for term in ["bhav", "ttab", "object error", "stack object", "bad gosub tree", "primitive", "opcode"]):
        summary = "Crash log contains gameplay/object-script terms. Treat hack or object-package conflicts as more likely than pure CAS problems."
        return "object-hack", summary, exception_module
    if any(term in lowered for term in ["startup", "failed to find", "could not initialize", "application has crashed", "failed to create"]):
        summary = "Crash happened during startup or initialization. Configuration, missing assets, and incompatible environment settings are likely contributors."
        return "startup", summary, exception_module
    summary = "Crash could not yet be strongly classified. Use the surrounding system/config and package diagnostics as supporting evidence."
    return "unknown", summary, exception_module


def parse_crash_log(path: Path) -> ParsedCrashReport:
    text = read_text(path)
    values = extract_key_values(text)
    log_type = detect_log_type(path, text)
    texture_memory_mb = parse_texture_memory(text, values)
    exception_module = parse_exception_module(text, values)
    exception_code = parse_exception_code(text, values)
    fault_address = parse_fault_address(text, values)
    crash_category, summary, memory_hint = classify_crash(log_type, text, values, exception_code, exception_module, texture_memory_mb)
    return ParsedCrashReport(
        source_path=str(path),
        file_name=path.name,
        log_type=log_type,
        sha256=sha256_text(text),
        occurred_at_text=parse_graphics_field(values, "time", "timestamp", "date", "occurred at"),
        app_name=parse_graphics_field(values, "application", "app name", "application name"),
        exception_code=exception_code,
        exception_module=exception_module,
        fault_address=fault_address,
        crash_category=crash_category,
        summary=summary,
        graphics_device=parse_graphics_field(values, "name (driver)", "graphics device info", "gpu", "renderer"),
        graphics_vendor=parse_graphics_field(values, "vendor", "vendor info", "vendor id"),
        driver_version=parse_graphics_field(values, "driver version", "driver"),
        texture_memory_mb=texture_memory_mb,
        os_version=parse_graphics_field(values, "os version", "os", "system"),
        memory_hint=memory_hint,
        raw_text=text,
    )
