from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.dbpf_parser import DBPFParseError, parse_dbpf
from tests.fixtures import write_package


class DBPFParserTests(unittest.TestCase):
    def test_parses_index_minor_one_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            package_path = Path(tmpdir) / "sample.package"
            write_package(
                package_path,
                [
                    {
                        "type_id": 0xAC506764,
                        "group_id": 0x1,
                        "instance_hi": 0x2,
                        "instance_id": 0x3,
                        "body": b"abc123",
                    }
                ],
                index_minor=1,
            )

            package = parse_dbpf(package_path)

            self.assertEqual(package.index_major, 7)
            self.assertEqual(package.index_minor, 1)
            self.assertEqual(len(package.resources), 1)
            self.assertEqual(package.resources[0].resource_key, "AC506764:00000001:00000002:00000003")
            self.assertEqual(package.resources[0].body, b"abc123")

    def test_parses_index_minor_zero_package(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            package_path = Path(tmpdir) / "sample_minor0.package"
            write_package(
                package_path,
                [
                    {
                        "type_id": 0x4F424A44,
                        "group_id": 0xA,
                        "instance_id": 0xB,
                        "body": b"objd-body",
                    }
                ],
                index_minor=0,
            )

            package = parse_dbpf(package_path)

            self.assertEqual(package.index_minor, 0)
            self.assertEqual(len(package.resources), 1)
            self.assertEqual(package.resources[0].instance_hi, None)
            self.assertEqual(package.resources[0].resource_key, "4F424A44:0000000A:00000000:0000000B")

    def test_rejects_truncated_header(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            package_path = Path(tmpdir) / "broken.package"
            package_path.write_bytes(b"DBPF")
            with self.assertRaises(DBPFParseError):
                parse_dbpf(package_path)

    def test_rejects_index_outside_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            package_path = Path(tmpdir) / "broken.package"
            blob = bytearray(96)
            blob[0:4] = b"DBPF"
            blob[32:36] = (7).to_bytes(4, "little")
            blob[36:40] = (1).to_bytes(4, "little")
            blob[40:44] = (400).to_bytes(4, "little")
            blob[44:48] = (24).to_bytes(4, "little")
            blob[60:64] = (1).to_bytes(4, "little")
            package_path.write_bytes(bytes(blob))
            with self.assertRaises(DBPFParseError):
                parse_dbpf(package_path)

    def test_rejects_unsupported_index_major(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            package_path = Path(tmpdir) / "unsupported_major.package"
            blob = bytearray(96)
            blob[0:4] = b"DBPF"
            blob[32:36] = (6).to_bytes(4, "little")
            blob[36:40] = (0).to_bytes(4, "little")
            blob[40:44] = (96).to_bytes(4, "little")
            blob[44:48] = (0).to_bytes(4, "little")
            blob[60:64] = (1).to_bytes(4, "little")
            package_path.write_bytes(bytes(blob))
            with self.assertRaises(DBPFParseError):
                parse_dbpf(package_path)

    def test_rejects_unsupported_index_minor(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            package_path = Path(tmpdir) / "unsupported_minor.package"
            blob = bytearray(96)
            blob[0:4] = b"DBPF"
            blob[32:36] = (7).to_bytes(4, "little")
            blob[36:40] = (0).to_bytes(4, "little")
            blob[40:44] = (96).to_bytes(4, "little")
            blob[44:48] = (0).to_bytes(4, "little")
            blob[60:64] = (9).to_bytes(4, "little")
            package_path.write_bytes(bytes(blob))
            with self.assertRaises(DBPFParseError):
                parse_dbpf(package_path)


if __name__ == "__main__":
    unittest.main()
