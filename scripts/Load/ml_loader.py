from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pandas as pd
from sklearn.model_selection import train_test_split

from .file_loader import ParquetLoader
from ..data_utils import log_action


class MLDataLoader(ParquetLoader):
    """
    Prépare et sauvegarde les fichiers train/test pour ton modèle.
    """
    @log_action("Saving ML train/test datasets")
    def _load_impl(
        self,
        df: pd.DataFrame,
        target_col: str,
        test_size: float = 0.2,
        random_state: int = 42,
        **_,
    ) -> Path:
        if target_col not in df.columns:
            raise ValueError(f"Colonne cible '{target_col}' introuvable dans le DataFrame.")

        X = df.drop(columns=[target_col])
        y = df[target_col]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )

        base = self.output_dir

        X_train_path = base / "X_train.parquet"
        X_test_path = base / "X_test.parquet"
        y_train_path = base / "y_train.parquet"
        y_test_path = base / "y_test.parquet"

        X_train.to_parquet(X_train_path, index=False)
        X_test.to_parquet(X_test_path, index=False)
        y_train.to_parquet(y_train_path, index=False)
        y_test.to_parquet(y_test_path, index=False)

        print("Jeux ML sauvegardés dans :", base)
        return base  # on renvoie le dossier
