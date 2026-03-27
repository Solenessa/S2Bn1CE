from __future__ import annotations

import struct
from pathlib import Path


def build_dbpf(resources: list[dict[str, object]], *, index_minor: int = 1) -> bytes:
    entry_size = 24 if index_minor in {1, 2} else 20
    header_size = 96
    body_offset = header_size
    bodies = bytearray()
    index_entries = bytearray()

    for resource in resources:
        body = bytes(resource["body"])
        file_offset = body_offset + len(bodies)
        bodies.extend(body)
        type_id = int(resource["type_id"])
        group_id = int(resource.get("group_id", 0))
        instance_hi = int(resource.get("instance_hi", 0))
        instance_id = int(resource.get("instance_id", 0))
        if index_minor == 0:
            index_entries.extend(struct.pack("<IIIII", type_id, group_id, instance_id, file_offset, len(body)))
        else:
            index_entries.extend(struct.pack("<IIIIII", type_id, group_id, instance_hi, instance_id, file_offset, len(body)))

    index_offset = header_size + len(bodies)
    header = bytearray(header_size)
    header[0:4] = b"DBPF"
    struct.pack_into("<I", header, 4, 1)
    struct.pack_into("<I", header, 8, 1)
    struct.pack_into("<I", header, 32, 7)
    struct.pack_into("<I", header, 36, len(resources))
    struct.pack_into("<I", header, 40, index_offset)
    struct.pack_into("<I", header, 44, len(index_entries))
    struct.pack_into("<I", header, 60, index_minor)
    return bytes(header + bodies + index_entries)


def make_3idr_body(target_type_id: int, target_group_id: int, target_instance_hi: int, target_instance_id: int) -> bytes:
    return struct.pack(
        "<III4I",
        0,
        1,
        1,
        target_type_id,
        target_group_id,
        target_instance_hi,
        target_instance_id,
    )


def make_txmt_body(*tokens: str) -> bytes:
    return ("\x00".join(tokens) + "\x00").encode("latin-1")


def write_package(path: Path, resources: list[dict[str, object]], *, index_minor: int = 1) -> None:
    path.write_bytes(build_dbpf(resources, index_minor=index_minor))
