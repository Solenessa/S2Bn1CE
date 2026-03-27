from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.bootstrap_db import initialize_database
from app.db import connect
from app.dbpf_parser import parse_dbpf
from app.diagnostics import (
    fetch_dependency_candidates,
    fetch_duplicate_mesh_candidates,
    fetch_orphan_recolor_groups,
    fetch_package_profiles,
)
from app.resource_parsers import parse_3idr, parse_gzps, parse_txmt
from app.scan_content import scan_root
from tests.real_samples import REAL_SAMPLE_DIRS, REAL_SAMPLE_ROOT, VEST_MESH_PACKAGE, VEST_RECOLOR_PACKAGE, VEST_SET_ROOT, real_samples_available


@unittest.skipUnless(real_samples_available(), "real Sims2 sample packages are not available")
class RealPackageCorpusTests(unittest.TestCase):
    def test_mesh_and_recolor_packages_parse_expected_resource_types(self) -> None:
        mesh_package = parse_dbpf(VEST_MESH_PACKAGE)
        recolor_package = parse_dbpf(VEST_RECOLOR_PACKAGE)

        self.assertEqual(sorted(resource.type_label for resource in mesh_package.resources), ["CRES", "GMDC", "GMND", "SHPE"])
        self.assertIn("3IDR", [resource.type_label for resource in recolor_package.resources])
        self.assertIn("GZPS", [resource.type_label for resource in recolor_package.resources])
        self.assertIn("TXMT", [resource.type_label for resource in recolor_package.resources])
        self.assertIn("TXTR", [resource.type_label for resource in recolor_package.resources])

    def test_real_recolor_package_yields_expected_parser_signals(self) -> None:
        package = parse_dbpf(VEST_RECOLOR_PACKAGE)
        parsed_3idr = None
        parsed_gzps = None
        parsed_txmt = None
        for resource in package.resources:
            if resource.type_label == "3IDR":
                parsed_3idr = parse_3idr(resource.body)
            elif resource.type_label == "GZPS":
                parsed_gzps = parse_gzps(resource.body)
            elif resource.type_label == "TXMT":
                parsed_txmt = parse_txmt(resource.body)

        self.assertIsNotNone(parsed_3idr)
        self.assertIsNotNone(parsed_gzps)
        self.assertIsNotNone(parsed_txmt)
        assert parsed_3idr is not None
        assert parsed_gzps is not None
        assert parsed_txmt is not None

        self.assertGreaterEqual(len(parsed_3idr.references), 2)
        self.assertEqual(parsed_gzps.name, "cubodypirate_redstripe")
        self.assertEqual(parsed_gzps.type_value, "skin")
        self.assertTrue(parsed_txmt.resource_name.startswith("##0x"))

    def test_real_folder_scan_finds_expected_dependency_edge(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "real-corpus.db"
            initialize_database(db_path)
            scan_root(VEST_SET_ROOT, db_path)

            connection = connect(db_path)
            profiles = {profile["file_name"]: profile for profile in fetch_package_profiles(connection, limit=100)}
            dependency_candidates, unresolved, signatures = fetch_dependency_candidates(connection)

            mesh_profile = profiles[VEST_MESH_PACKAGE.name]
            recolor_profile = profiles[VEST_RECOLOR_PACKAGE.name]
            top_candidate = dependency_candidates[recolor_profile["id"]][0]

            self.assertEqual(mesh_profile["category"], "Mesh Package")
            self.assertEqual(recolor_profile["category"], "Recolor or Property Set")
            self.assertEqual(top_candidate["mesh_name"], VEST_MESH_PACKAGE.name)
            self.assertGreaterEqual(top_candidate["score"], 18)
            self.assertFalse(any(item["id"] == recolor_profile["id"] for item in unresolved))
            self.assertIn("cubodypirate", signatures[recolor_profile["id"]]["normalized_names"])
            connection.close()

    def test_full_real_corpus_has_expected_folder_structure_and_dependency_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "real-corpus-full.db"
            initialize_database(db_path)
            scan_root(REAL_SAMPLE_ROOT, db_path)

            connection = connect(db_path)
            profiles = fetch_package_profiles(connection, limit=5000)
            dependency_candidates, unresolved, _ = fetch_dependency_candidates(connection)
            profiles_by_id = {profile["id"]: profile for profile in profiles}

            self.assertEqual(len(unresolved), 0)
            self.assertEqual(len(fetch_orphan_recolor_groups(connection)), 0)

            for folder in REAL_SAMPLE_DIRS:
                relative_folder = folder.name
                folder_profiles = [
                    profile for profile in profiles if Path(profile["relative_path"]).parent.as_posix() == relative_folder
                ]
                meshes = [profile for profile in folder_profiles if profile["category"] == "Mesh Package"]
                recolors = [profile for profile in folder_profiles if profile["category"] == "Recolor or Property Set"]
                self.assertEqual(len(meshes), 1, relative_folder)
                self.assertEqual(len(recolors), 6, relative_folder)
                mesh_name = meshes[0]["file_name"]
                for recolor in recolors:
                    top_candidate = dependency_candidates[recolor["id"]][0]
                    self.assertEqual(top_candidate["mesh_name"], mesh_name, recolor["file_name"])
                    self.assertGreaterEqual(top_candidate["score"], 18, recolor["file_name"])

            duplicate_mesh_groups = fetch_duplicate_mesh_candidates(connection)
            self.assertEqual(len(duplicate_mesh_groups), 1)
            self.assertEqual(duplicate_mesh_groups[0]["file_name"], "MESH_fp7_culshoodie_080708.package")
            self.assertTrue(duplicate_mesh_groups[0]["exact_duplicate"])
            self.assertEqual(duplicate_mesh_groups[0]["copy_count"], 2)
            connection.close()


if __name__ == "__main__":
    unittest.main()
