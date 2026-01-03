from pathlib import Path

import pandas as pd
import pytest

from ..base_loader import BaseLoader


class DummyLoader(BaseLoader):
    """Implémentation minimale pour tester BaseLoader."""

    def _load_impl(self, df: pd.DataFrame, **kwargs) -> Path:
        path = self._build_path(ext="csv", filename="dummy.csv", timestamped=False)
        df.to_csv(path, index=False)
        return path


def test_base_loader_rejects_non_dataframe(tmp_output_dir):
    loader = DummyLoader(output_dir=tmp_output_dir)

    with pytest.raises(TypeError):
        loader.load(df="not a dataframe")  # type: ignore


def test_base_loader_rejects_empty_df(tmp_output_dir):
    loader = DummyLoader(output_dir=tmp_output_dir)
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError):
        loader.load(empty_df, allow_empty=False)


def test_base_loader_rejects_nan_when_not_allowed(tmp_output_dir):
    loader = DummyLoader(output_dir=tmp_output_dir)
    df = pd.DataFrame({"a": [1, None, 3]})

    with pytest.raises(ValueError):
        loader.load(df, allow_na=False)


def test_base_loader_accepts_valid_df(tmp_output_dir):
    loader = DummyLoader(output_dir=tmp_output_dir)
    df = pd.DataFrame({"a": [1, 2, 3]})

    out = loader.load(df)
    assert out.exists()
