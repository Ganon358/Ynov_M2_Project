from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from .base_loader import BaseLoader
from ..data_utils import log_action


class CsvLoader(BaseLoader):
    @log_action("Saving CSV file")
    def _load_impl(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        index: bool = False,
        sep: str = ",",
        timestamped: bool = False,
        encoding: str = "utf-8",
        **_,
    ) -> Path:
        path = self._build_path(
            ext="csv",
            filename=filename,
            timestamped=timestamped,
        )
        df.to_csv(path, index=index, sep=sep, encoding=encoding)
        print(f"CSV sauvegardé dans : {path}")
        return path


class ParquetLoader(BaseLoader):
    @log_action("Saving Parquet file")
    def _load_impl(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        timestamped: bool = False,
        **_,
    ) -> Path:
        path = self._build_path(
            ext="parquet",
            filename=filename,
            timestamped=timestamped,
        )
        df.to_parquet(path, index=False)
        print(f"Parquet sauvegardé dans : {path}")
        return path


class JsonLoader(BaseLoader):
    @log_action("Saving JSON file")
    def _load_impl(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        orient: str = "records",
        lines: bool = False,
        timestamped: bool = False,
        **_,
    ) -> Path:
        path = self._build_path(
            ext="json",
            filename=filename,
            timestamped=timestamped,
        )
        df.to_json(path, orient=orient, lines=lines, force_ascii=False)
        print(f"JSON sauvegardé dans : {path}")
        return path
