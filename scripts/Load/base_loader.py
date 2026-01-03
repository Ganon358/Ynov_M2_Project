from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from ..data_utils import log_action


class BaseLoader(ABC):
    def __init__(
        self,
        output_dir: str = "data/processed",
        file_prefix: str = "dataset",
        create_dir: bool = True,
    ) -> None:
        self.output_dir = Path(output_dir)
        if create_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        self.file_prefix = file_prefix

    def _build_path(
        self,
        ext: str = "csv",
        filename: Optional[str] = None,
        timestamped: bool = True,
    ) -> Path:
        if filename is not None:
            return self.output_dir / filename

        ts = datetime.now().strftime("%Y%m%d_%H%M%S") if timestamped else ""
        suffix = f"_{ts}" if ts else ""
        return self.output_dir / f"{self.file_prefix}{suffix}.{ext}"

    def _validate_df(
        self,
        df: pd.DataFrame,
        allow_empty: bool = False,
        allow_na: bool = True,
    ) -> None:
        if not isinstance(df, pd.DataFrame):
            raise TypeError("BaseLoader.load attend un pandas.DataFrame")

        if df.empty and not allow_empty:
            raise ValueError("Le DataFrame à charger est vide.")

        if not allow_na and df.isna().any().any():
            raise ValueError(
                "Le DataFrame contient encore des valeurs manquantes. "
                "Nettoie-les dans la partie transform avant de charger."
            )

    @log_action("Loading dataset")
    def load(self, df: pd.DataFrame, **kwargs) -> Path:
        """Méthode principale appelée par les tests / l’ETL."""
        self._validate_df(
            df,
            allow_empty=kwargs.pop("allow_empty", False),
            allow_na=kwargs.pop("allow_na", True),
        )
        return self._load_impl(df, **kwargs)

    # alias pour ne pas casser d’éventuels anciens imports
    def run(self, df: pd.DataFrame, **kwargs) -> Path:
        return self.load(df, **kwargs)

    @abstractmethod
    def _load_impl(self, df: pd.DataFrame, **kwargs) -> Path:
        ...
