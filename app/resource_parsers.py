#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass
import re
import struct


@dataclass
class ParsedOBJD:
    object_name: str
    version: int | None
    guid: int | None
    original_guid: int | None
    diagonal_guid: int | None
    grid_aligned_guid: int | None
    proxy_guid: int | None
    job_object_guid: int | None
    object_model_guid: int | None
    interaction_table_id: int | None
    object_type: int | None
    price: int | None
    slot_id: int | None
    catalog_strings_id: int | None
    function_sort_flags: int | None
    room_sort_flags: int | None
    expansion_flag: int | None
    multi_tile_master_id: int | None
    multi_tile_sub_index: int | None
    raw_length: int


@dataclass
class ParsedBHAV:
    function_name: str
    signature: int | None
    instruction_count: int | None
    tree_type: int | None
    arg_count: int | None
    local_var_count: int | None
    header_flag: int | None
    tree_version: int | None
    instruction_length: int
    first_opcode: int | None
    last_opcode: int | None
    raw_length: int


@dataclass
class ParsedTTAB:
    format_code: int | None
    raw_length: int


@dataclass
class ResourceRef:
    type_id: int
    group_id: int
    instance_hi: int
    instance_id: int

    @property
    def resource_key(self) -> str:
        return f"{self.type_id:08X}:{self.group_id:08X}:{self.instance_hi:08X}:{self.instance_id:08X}"


@dataclass
class Parsed3IDR:
    version: int | None
    entry_count: int
    references: list[ResourceRef]
    raw_length: int


@dataclass
class ParsedTXMT:
    resource_name: str
    material_class: str
    texture_name_candidates: list[str]
    raw_length: int


@dataclass
class ParsedGZPS:
    name: str
    creator: str
    family: str
    age: int | None
    gender: int | None
    species: int | None
    parts: int | None
    outfit: int | None
    flags: int | None
    product: int | None
    genetic: int | None
    type_value: str
    skintone: str
    hairtone: str
    category_bin: int | None
    raw_length: int


def read_u16(data: bytes, offset: int) -> int | None:
    if offset + 2 > len(data):
        return None
    return struct.unpack_from("<H", data, offset)[0]


def read_u32(data: bytes, offset: int) -> int | None:
    if offset + 4 > len(data):
        return None
    return struct.unpack_from("<I", data, offset)[0]


def read_c_string(data: bytes, max_len: int) -> str:
    end = data.find(b"\x00", 0, max_len)
    if end == -1:
        end = min(max_len, len(data))
    return data[:end].decode("latin-1", errors="replace").strip()


def parse_objd(resource_body: bytes) -> ParsedOBJD:
    return ParsedOBJD(
        object_name=read_c_string(resource_body, 64),
        version=read_u32(resource_body, 0x040),
        interaction_table_id=read_u16(resource_body, 0x04E),
        object_type=read_u16(resource_body, 0x052),
        multi_tile_master_id=read_u16(resource_body, 0x054),
        multi_tile_sub_index=read_u16(resource_body, 0x056),
        guid=read_u32(resource_body, 0x05C),
        price=read_u16(resource_body, 0x064),
        slot_id=read_u16(resource_body, 0x068),
        diagonal_guid=read_u32(resource_body, 0x06A),
        grid_aligned_guid=read_u32(resource_body, 0x06E),
        proxy_guid=read_u32(resource_body, 0x07A),
        room_sort_flags=read_u16(resource_body, 0x08E),
        function_sort_flags=read_u16(resource_body, 0x090),
        catalog_strings_id=read_u16(resource_body, 0x092),
        job_object_guid=read_u32(resource_body, 0x0A8),
        expansion_flag=read_u16(resource_body, 0x0C0),
        original_guid=read_u32(resource_body, 0x0CC),
        object_model_guid=read_u32(resource_body, 0x0D0),
        raw_length=len(resource_body),
    )


def parse_bhav(resource_body: bytes) -> ParsedBHAV:
    signature = read_u16(resource_body, 0x00)
    instruction_count = read_u16(resource_body, 0x02)
    tree_type = read_u8(resource_body, 0x04)
    arg_count = read_u8(resource_body, 0x05)
    local_var_count = read_u8(resource_body, 0x06)
    header_flag = read_u8(resource_body, 0x07)
    tree_version = read_u32(resource_body, 0x08)

    # Sims 2 BHAVs commonly store a 64-byte null-terminated name block after the fixed header.
    name_offset = 0x0C
    function_name = ""
    if len(resource_body) >= name_offset + 64:
        function_name = read_c_string(resource_body[name_offset : name_offset + 64], 64)

    instruction_length = 12
    instruction_base = 0x4C if len(resource_body) >= 0x4C else 0x0C
    if signature == 0x8007:
        instruction_length = 12
    elif signature == 0x8008:
        instruction_length = 16

    first_opcode = None
    last_opcode = None
    if instruction_count and instruction_count > 0:
        first_opcode = read_u16(resource_body, instruction_base)
        last_offset = instruction_base + ((instruction_count - 1) * instruction_length)
        last_opcode = read_u16(resource_body, last_offset)

    return ParsedBHAV(
        function_name=function_name,
        signature=signature,
        instruction_count=instruction_count,
        tree_type=tree_type,
        arg_count=arg_count,
        local_var_count=local_var_count,
        header_flag=header_flag,
        tree_version=tree_version,
        instruction_length=instruction_length,
        first_opcode=first_opcode,
        last_opcode=last_opcode,
        raw_length=len(resource_body),
    )


def read_u8(data: bytes, offset: int) -> int | None:
    if offset + 1 > len(data):
        return None
    return data[offset]


def parse_ttab(resource_body: bytes) -> ParsedTTAB:
    return ParsedTTAB(
        format_code=read_u32(resource_body, 0x00),
        raw_length=len(resource_body),
    )


def extract_printable_tokens(resource_body: bytes) -> list[str]:
    text = resource_body.decode("latin-1", errors="ignore")
    tokens: list[str] = []
    current = []
    for char in text:
        if 32 <= ord(char) < 127:
            current.append(char)
        else:
            if len(current) >= 4:
                tokens.append("".join(current))
            current = []
    if len(current) >= 4:
        tokens.append("".join(current))
    return tokens


def parse_3idr(resource_body: bytes) -> Parsed3IDR:
    version = read_u32(resource_body, 0x04)
    entry_count = read_u32(resource_body, 0x08) or 0
    references: list[ResourceRef] = []
    offset = 0x0C
    for _ in range(entry_count):
        if offset + 16 > len(resource_body):
            break
        references.append(
            ResourceRef(
                type_id=read_u32(resource_body, offset) or 0,
                group_id=read_u32(resource_body, offset + 4) or 0,
                instance_hi=read_u32(resource_body, offset + 8) or 0,
                instance_id=read_u32(resource_body, offset + 12) or 0,
            )
        )
        offset += 16
    return Parsed3IDR(
        version=version,
        entry_count=entry_count,
        references=references,
        raw_length=len(resource_body),
    )


def parse_txmt(resource_body: bytes) -> ParsedTXMT:
    tokens = extract_printable_tokens(resource_body)
    resource_name = next((token for token in tokens if token.startswith("##0x") and "!body_" in token), "")
    material_class = next((token for token in tokens if token in {"SimSkin", "StandardMaterial", "MaterialDefinition"}), "")
    texture_name_candidates = [
        token
        for token in tokens
        if any(marker in token.lower() for marker in ["body", "txmt", "texture", "pirate", "hoodie", "dress", "vest"])
    ]
    return ParsedTXMT(
        resource_name=resource_name,
        material_class=material_class,
        texture_name_candidates=texture_name_candidates[:12],
        raw_length=len(resource_body),
    )


def _read_gzps_value(resource_body: bytes, key: str) -> bytes | None:
    marker = key.encode("latin-1", errors="ignore")
    index = resource_body.find(marker)
    if index < 0 or index < 8:
        return None
    length = read_u32(resource_body, index + len(marker))
    if length is None:
        return None
    value_offset = index + len(marker) + 4
    if value_offset + length > len(resource_body):
        return None
    return resource_body[value_offset : value_offset + length]


def _gzps_string(resource_body: bytes, key: str) -> str:
    value = _read_gzps_value(resource_body, key)
    if not value:
        return ""
    return value.decode("latin-1", errors="ignore").rstrip("\x00")


def _gzps_u32(resource_body: bytes, key: str) -> int | None:
    value = _read_gzps_value(resource_body, key)
    if not value:
        return None
    if len(value) >= 4:
        return struct.unpack_from("<I", value, 0)[0]
    if len(value) == 2:
        return struct.unpack_from("<H", value, 0)[0]
    if len(value) == 1:
        return value[0]
    return None


def _gzps_fuzzy_string(resource_body: bytes, key: str, minimum: int = 4) -> str:
    text = resource_body.decode("latin-1", errors="ignore")
    index = text.lower().find(key.lower())
    if index < 0:
        return ""
    window = text[index + len(key) : index + len(key) + 160]
    if key == "creator":
        match = re.search(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", window)
        return match.group(0) if match else ""
    if key in {"family", "skintone", "hairtone"}:
        match = re.search(r"(?:[0-9a-fA-F]{8}-){3}[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", window)
        return match.group(0) if match else ""
    tokens = re.findall(r"[A-Za-z0-9_:-]{%d,80}" % minimum, window)
    for token in tokens:
        lowered = token.lower()
        if lowered == key.lower():
            continue
        if key == "type" and lowered not in {"skin", "hair", "outfit", "clothing"}:
            continue
        return token
    return ""


def _sane_small_int(value: int | None, limit: int = 255) -> int | None:
    if value is None:
        return None
    if 0 <= value <= limit:
        return value
    return None


def parse_gzps(resource_body: bytes) -> ParsedGZPS:
    name = _gzps_string(resource_body, "name") or _gzps_fuzzy_string(resource_body, "name", minimum=6)
    creator = _gzps_string(resource_body, "creator") or _gzps_fuzzy_string(resource_body, "creator", minimum=8)
    family = _gzps_string(resource_body, "family") or _gzps_fuzzy_string(resource_body, "family", minimum=8)
    type_value = _gzps_string(resource_body, "type") or _gzps_fuzzy_string(resource_body, "type", minimum=4)
    skintone = _gzps_string(resource_body, "skintone") or _gzps_fuzzy_string(resource_body, "skintone", minimum=8)
    hairtone = _gzps_string(resource_body, "hairtone") or _gzps_fuzzy_string(resource_body, "hairtone", minimum=8)
    return ParsedGZPS(
        name=name,
        creator=creator,
        family=family,
        age=_sane_small_int(_gzps_u32(resource_body, "age"), limit=255),
        gender=_sane_small_int(_gzps_u32(resource_body, "gender"), limit=255),
        species=_sane_small_int(_gzps_u32(resource_body, "species"), limit=255),
        parts=_sane_small_int(_gzps_u32(resource_body, "parts"), limit=1_000_000),
        outfit=_sane_small_int(_gzps_u32(resource_body, "outfit"), limit=1_000_000),
        flags=_sane_small_int(_gzps_u32(resource_body, "flags"), limit=1_000_000),
        product=_sane_small_int(_gzps_u32(resource_body, "product"), limit=1_000_000),
        genetic=_sane_small_int(_gzps_u32(resource_body, "genetic"), limit=1_000_000),
        type_value=type_value,
        skintone=skintone,
        hairtone=hairtone,
        category_bin=_sane_small_int(_gzps_u32(resource_body, "categorybin"), limit=1_000_000),
        raw_length=len(resource_body),
    )
