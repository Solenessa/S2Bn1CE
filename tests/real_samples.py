from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_SAMPLE_ROOT = REPO_ROOT.parent / "Sims2-Test-Packages"
REAL_SAMPLE_DIRS = sorted(path for path in REAL_SAMPLE_ROOT.iterdir() if path.is_dir()) if REAL_SAMPLE_ROOT.exists() else []
VEST_SET_ROOT = REAL_SAMPLE_ROOT / "MTS_fakepeeps7_784849_boysvestoutfits-all6coloursmesh"
VEST_MESH_PACKAGE = VEST_SET_ROOT / "MESH_fp7_cmbodyvest_080709.package"
VEST_RECOLOR_PACKAGE = VEST_SET_ROOT / "5f04dc71_chMvest04.package"


def real_samples_available() -> bool:
    return bool(REAL_SAMPLE_DIRS) and VEST_MESH_PACKAGE.exists() and VEST_RECOLOR_PACKAGE.exists()
