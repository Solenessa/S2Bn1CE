#!/usr/bin/env python3

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import struct


DIR_TYPE_ID = 0xE86B1EEF


TYPE_LABELS = {
    0x0C560F39: "XOBJ",
    0x1C4A276C: "TXTR",
    0x49596978: "TXMT",
    0x7BA3838C: "GMND",
    0xAC4F8687: "GMDC",
    0xAC506764: "3IDR",
    0x42484156: "BHAV",
    0x42434F4E: "BCON",
    0x43545453: "CTSS",
    0x474C4F42: "GLOB",
    0x4F424A44: "OBJD",
    0x53545223: "STR#",
    0x54444154: "TDAT",
    0x54544142: "TTAB",
    0x54544153: "TTAs",
    0x54505250: "TPRP",
    0x4D4D4154: "MMAT",
    0x54455854: "TEXT",
    0x4A504547: "JPEG",
    0x4C494645: "LIFO",
    0xE519C933: "CRES",
    0xEBCF3E27: "GZPS",
    0xFC6EB1F7: "SHPE",
    DIR_TYPE_ID: "DIR",
}


class DBPFParseError(Exception):
    pass


@dataclass
class DBPFResource:
    type_id: int
    group_id: int
    instance_id: int
    instance_hi: int | None
    file_offset: int
    file_size: int
    body_sha256: str | None = None
    body: bytes | None = None

    @property
    def resource_key(self) -> str:
        hi = self.instance_hi or 0
        return f"{self.type_id:08X}:{self.group_id:08X}:{hi:08X}:{self.instance_id:08X}"

    @property
    def type_label(self) -> str:
        return TYPE_LABELS.get(self.type_id, f"0x{self.type_id:08X}")

    @property
    def is_dir_record(self) -> bool:
        return self.type_id == DIR_TYPE_ID


@dataclass
class DBPFPackage:
    dbpf_major: int
    dbpf_minor: int
    index_major: int
    index_minor: int
    resources: list[DBPFResource]


def _u32(buffer: bytes, offset: int) -> int:
    return struct.unpack_from("<I", buffer, offset)[0]


def parse_dbpf(path: Path) -> DBPFPackage:
    data = path.read_bytes()
    if len(data) < 96:
        raise DBPFParseError("File too small to be a DBPF package.")
    if data[:4] != b"DBPF":
        raise DBPFParseError("Missing DBPF magic header.")

    dbpf_major = _u32(data, 4)
    dbpf_minor = _u32(data, 8)
    index_major = _u32(data, 32)
    index_count = _u32(data, 36)
    index_offset = _u32(data, 40)
    index_size = _u32(data, 44)
    index_minor = _u32(data, 60)

    if index_offset + index_size > len(data):
        raise DBPFParseError("Index table points outside the file.")
    if index_major != 7:
        raise DBPFParseError(f"Unsupported DBPF index major version: {index_major}")

    index_blob = data[index_offset : index_offset + index_size]
    if index_minor == 0:
        entry_size = 20
    elif index_minor in {1, 2}:
        entry_size = 24
    else:
        raise DBPFParseError(f"Unsupported DBPF index minor version: {index_minor}")

    expected_size = index_count * entry_size
    if expected_size > len(index_blob):
        raise DBPFParseError("Index entry count exceeds index blob length.")

    resources: list[DBPFResource] = []
    cursor = 0
    for _ in range(index_count):
        type_id = _u32(index_blob, cursor)
        group_id = _u32(index_blob, cursor + 4)
        if index_minor == 0:
            instance_hi = None
            instance_id = _u32(index_blob, cursor + 8)
            file_offset = _u32(index_blob, cursor + 12)
            file_size = _u32(index_blob, cursor + 16)
        else:
            instance_hi = _u32(index_blob, cursor + 8)
            instance_id = _u32(index_blob, cursor + 12)
            file_offset = _u32(index_blob, cursor + 16)
            file_size = _u32(index_blob, cursor + 20)
        resources.append(
            DBPFResource(
                type_id=type_id,
                group_id=group_id,
                instance_id=instance_id,
                instance_hi=instance_hi,
                file_offset=file_offset,
                file_size=file_size,
                body_sha256=_hash_resource_body(data, file_offset, file_size),
                body=_slice_resource_body(data, file_offset, file_size),
            )
        )
        cursor += entry_size

    return DBPFPackage(
        dbpf_major=dbpf_major,
        dbpf_minor=dbpf_minor,
        index_major=index_major,
        index_minor=index_minor,
        resources=resources,
    )


def _hash_resource_body(data: bytes, file_offset: int, file_size: int) -> str | None:
    body = _slice_resource_body(data, file_offset, file_size)
    if body is None:
        return None
    return hashlib.sha256(body).hexdigest()


def _slice_resource_body(data: bytes, file_offset: int, file_size: int) -> bytes | None:
    if file_offset < 0 or file_size < 0 or file_offset + file_size > len(data):
        return None
    return data[file_offset : file_offset + file_size]
