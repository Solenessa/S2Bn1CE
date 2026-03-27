from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.bootstrap_db import initialize_database
from app.db import connect
from app.scan_content import scan_root
from tests.fixtures import make_3idr_body, make_txmt_body, write_package


class ScanPipelineTests(unittest.TestCase):
    def test_scan_persists_dependency_metadata_without_runtime_reparse(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "downloads"
            root.mkdir()
            db_path = Path(tmpdir) / "diag.db"
            initialize_database(db_path)

            mesh_key = {
                "type_id": 0xE519C933,
                "group_id": 0x10,
                "instance_hi": 0x20,
                "instance_id": 0x30,
            }
            write_package(
                root / "mesh.package",
                [
                    {
                        **mesh_key,
                        "body": b"mesh.body_alpha_gmdc\x00",
                    }
                ],
            )
            write_package(
                root / "recolor.package",
                [
                    {
                        "type_id": 0xAC506764,
                        "group_id": 0x99,
                        "instance_hi": 0,
                        "instance_id": 1,
                        "body": make_3idr_body(
                            mesh_key["type_id"],
                            mesh_key["group_id"],
                            mesh_key["instance_hi"],
                            mesh_key["instance_id"],
                        ),
                    },
                    {
                        "type_id": 0x49596978,
                        "group_id": 0x99,
                        "instance_hi": 0,
                        "instance_id": 2,
                        "body": make_txmt_body("##0x12345678!body_alpha_txmt", "StandardMaterial"),
                    },
                ],
            )

            result = scan_root(root, db_path)

            self.assertEqual(result["parsed_packages"], 2)
            connection = connect(db_path)
            link_count = connection.execute("SELECT COUNT(*) FROM resource_links").fetchone()[0]
            hint_count = connection.execute("SELECT COUNT(*) FROM scenegraph_names").fetchone()[0]
            self.assertGreaterEqual(link_count, 1)
            self.assertGreaterEqual(hint_count, 1)
            connection.close()

    def test_initialize_database_is_non_destructive_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "diag.db"
            initialize_database(db_path)
            connection = connect(db_path)
            connection.execute("INSERT INTO scan_runs (root_path) VALUES (?)", ("/tmp/example",))
            connection.commit()
            connection.close()

            initialize_database(db_path)

            connection = connect(db_path)
            count = connection.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0]
            connection.close()
            self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
