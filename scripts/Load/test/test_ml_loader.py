from pathlib import Path

from ..ml_loader import MLDataLoader


def test_ml_loader_creates_train_test_files(products_df, tmp_output_dir):
    df = products_df.copy()

    # Si tu as déjà une vraie colonne cible, remplace 'target_test' par son nom.
    df["target_test"] = 0
    if not df.empty:
        df.loc[df.index[: len(df) // 2], "target_test"] = 1

    loader = MLDataLoader(
        output_dir=tmp_output_dir,
        file_prefix="ml_products_test",
    )

    base_path: Path = loader.load(
        df,
        target_col="target_test",
        test_size=0.2,
        random_state=42,
    )

    expected_files = [
        base_path / "X_train.parquet",
        base_path / "X_test.parquet",
        base_path / "y_train.parquet",
        base_path / "y_test.parquet",
    ]

    for f in expected_files:
        assert f.exists(), f"{f} n'a pas été créé"
