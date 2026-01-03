from pathlib import Path

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def project_root() -> Path:
    """
    Racine du projet interne (le second 'Ynov_M2_Project-main').
    __file__ → test/ -> Load -> scripts -> Ynov_M2_Project-main (interne)
    """
    return Path(__file__).resolve().parents[3]


@pytest.fixture(scope="session")
def products_csv_path(project_root: Path) -> Path:
    path = project_root / "data" / "processed" / "products_clean.csv"
    if not path.exists():
        raise FileNotFoundError(f"products_clean.csv introuvable : {path}")
    return path


@pytest.fixture(scope="session")
def products_df(products_csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(products_csv_path)
    return df


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    out = tmp_path / "out"
    out.mkdir(parents=True, exist_ok=True)
    return out
