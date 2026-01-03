from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd

from ..data_utils import load_data
from ..transform.cleaner.data_missing_values import AdvancedDataFrameProcessor
from ..transform.cleaner.data_outliers import OutlierDetection
from .file_loader import CsvLoader
from .ml_loader import MLDataLoader


async def run_etl_and_load(raw_file: str, target_col: str) -> None:
    # ----------- 1. Chargement brut -----------
    df_raw = load_data(raw_file, limit=None)

    # ----------- 2. TRANSFORM : nettoyage / imputations -----------
    processor = AdvancedDataFrameProcessor(df_import=df_raw)
    await processor.process_dataframe()
    processor.impute_missing_values(method="knn")  # à adapter selon le besoin
    processor.filter_irrelevant_columns()

    # Outliers
    outlier_detector = OutlierDetection(df=processor.df)
    outlier_detector.run_outlier_analysis(strategy="clip")
    df_clean = outlier_detector.df

    # ----------- 3. LOAD vers fichier final -----------
    csv_loader = CsvLoader(
        output_dir="data/processed",
        file_prefix="products_clean",
    )

    final_csv_path = csv_loader.run(
        df_clean,
        filename="products_clean.csv",
        timestamped=False,
        allow_na=False,  # si tu veux forcer l’absence de NaN
    )

    # ----------- 4. (optionnel) préparation data pour l’IA -----------
    ml_loader = MLDataLoader(
        output_dir="data/processed/ml",
        file_prefix="ml_dataset",
    )
    ml_loader.run(df_clean, target_col=target_col)

    print("\nETL terminé.")
    print(f"Fichier propre : {final_csv_path}")


if __name__ == "__main__":
    # Exemple : python -m scripts.Load.run_load
    asyncio.run(
        run_etl_and_load(
            raw_file="data/raw/all_breweries_20250101_101010.json",
            target_col="label_nutrition"  # à adapter à ton cas
        )
    )
