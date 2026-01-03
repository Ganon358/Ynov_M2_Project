from pathlib import Path

import pandas as pd

from ..file_loader import CsvLoader


def test_csv_loader_writes_file(products_df, tmp_output_dir):
    loader = CsvLoader(
        output_dir=tmp_output_dir,
        file_prefix="products_test",
    )

    output_path: Path = loader.load(
        products_df.copy(),
        filename="products_test.csv",
        timestamped=False,
        allow_na=True,   # on accepte les NaN dans ce test
    )

    # 1. Fichier créé
    assert output_path.exists()

    # 2. Relire le CSV
    df_loaded = pd.read_csv(output_path)

    # 3. Vérifier structure
    assert list(df_loaded.columns) == list(products_df.columns)
    assert len(df_loaded) == len(products_df)
