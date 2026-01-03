"""
ETL - TRANSFORM : Normalisation des produits OpenFoodFacts

Objectif :
    - Charger les données brutes extraites (JSONL)
    - Structurer les colonnes (nutriments, poids, listes → strings)
    - Nettoyer / normaliser les types
    - Gérer les valeurs manquantes de façon raisonnable
    - Produire un CSV propre pour la phase LOAD (base SQL)

Ce script est spécifique aux produits alimentaires OpenFoodFacts.
"""

import asyncio
import re
from pathlib import Path
from typing import List, Tuple, Optional

import pandas as pd

# --------------------------------------------------------------------
# Configuration des chemins
# --------------------------------------------------------------------

THIS_DIR = Path(__file__).resolve().parent          # .../scripts/transform
SCRIPTS_DIR = THIS_DIR.parent                       # .../scripts
ROOT = SCRIPTS_DIR.parent                           # .../Ynov_M2_Project-main

SCRAPER_OUT_DIR = SCRIPTS_DIR / "extract" / "scraper" / "out"
DATA_PROCESSED_DIR = ROOT / "data" / "processed"
OUTPUT_FILE = DATA_PROCESSED_DIR / "products_clean.csv"

# Import du processeur avancé (nettoyage générique)
from .cleaner.data_missing_values import AdvancedDataFrameProcessor  # type: ignore


# ====================================================================
#                1. Chargement des fichiers bruts JSONL
# ====================================================================

def find_jsonl_files() -> List[Path]:
    """
    Retourne la liste des fichiers JSONL produits par le scraper.
    On cherche les fichiers au format : products_*.jsonl
    """
    files = sorted(SCRAPER_OUT_DIR.glob("products_*.jsonl"))
    if not files:
        raise FileNotFoundError(
            f"Aucun fichier products_*.jsonl trouvé dans {SCRAPER_OUT_DIR}"
        )
    return files


# ====================================================================
#                2. Transformations spécifiques produits
# ====================================================================

def expand_nutriments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Éclate la colonne 'nutriments' (dict) en colonnes séparées :
    - protides
    - glucides
    - lipides
    - calories
    """
    if "nutriments" not in df.columns:
        return df

    nutriments_df = df["nutriments"].apply(
        lambda x: x if isinstance(x, dict) else {}
    ).apply(pd.Series)

    nutriments_df = nutriments_df.rename(
        columns={
            "protides": "protides",
            "glucides": "glucides",
            "lipides": "lipides",
            "calories": "calories",
        }
    )

    df = df.drop(columns=["nutriments"])
    df = pd.concat([df, nutriments_df], axis=1)

    # On force ces colonnes en numérique (NaN si non convertible)
    for col in ["protides", "glucides", "lipides", "calories"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def list_to_str(x):
    """
    Convertit les listes (brands_tags, categories, etc.) en chaîne de caractères.
    Exemple : ["A", "B", "C"] → "A, B, C"
    """
    if isinstance(x, list):
        return ", ".join(str(v) for v in x)
    return x


def parse_weight(value: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    """
    Extrait un poids + unité depuis une chaîne comme :
        - "400 g"
        - "1500 ml"
        - "400 g (280 g net égoutté)"

    Retourne :
        (valeur_normalisée, unité_normalisée)
        - unités normalisées : g / ml
        - retourne (None, None) si non parsable
    """
    if not isinstance(value, str):
        return None, None

    match = re.search(r"(\d+(?:[\s.,]\d+)*)\s*(g|kg|ml|l)", value, flags=re.IGNORECASE)
    if not match:
        return None, None

    number = match.group(1).replace(" ", "").replace(",", ".")
    unit = match.group(2).lower()

    try:
        val = float(number)
    except ValueError:
        return None, unit

    # Normalisation : tout en g / ml
    if unit == "kg":
        val *= 1000
        unit = "g"
    if unit == "l":
        val *= 1000
        unit = "ml"

    return val, unit


def base_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformations "métier" spécifiques OpenFoodFacts :
      - explosion nutriments
      - conversion des listes en strings
      - extraction du poids en valeur + unité normalisée
      - suppression des doublons sur le code-barres
      - nettoyage de quelques colonnes techniques
    """
    # 1) nutriments -> colonnes numériques
    df = expand_nutriments(df)

    # 2) listes -> chaînes de caractères
    for col in ["categories", "brands_tags"]:
        if col in df.columns:
            df[col] = df[col].apply(list_to_str)

    # 3) poids -> poids_valeur / poids_unite_norm
    if "poids_unité" in df.columns:
        weights = df["poids_unité"].apply(parse_weight)
        df["poids_valeur"], df["poids_unite_norm"] = zip(*weights)

    # 4) suppression des doublons sur code_barres
    if "code_barres" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["code_barres"])
        print(f"Doublons supprimés sur 'code_barres' : {before - len(df)}")

    # 5) suppression éventuelle de colonnes purement techniques
    # 'index' = duplicat de code_barres dans ton cas → on peut le supprimer
    if "index" in df.columns and "code_barres" in df.columns:
        df = df.drop(columns=["index"])

    return df


# ====================================================================
#                3. Nettoyage avancé avec ton module
# ====================================================================

async def advanced_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage avancé en s'appuyant sur AdvancedDataFrameProcessor :

      1. Classification colonnes (numériques / catégorielles)
      2. Conversion en numérique des colonnes numériques
      3. Imputation des colonnes numériques par la médiane
         (mais laisse les nutriments vides si aucune info)
      4. Imputation des colonnes textuelles par 'Unknown'
         seulement pour les champs descriptifs / géographiques
      5. Suppression des colonnes trop vides (> 80 % de NaN)
    """
    processor = AdvancedDataFrameProcessor(df_import=df)

    # 1) typage + résumé mémoire
    df = await processor.process_dataframe()

    # 2) colonnes numériques : on s'assure qu'elles sont bien en float
    num_cols = processor.numeric_columns
    if num_cols:
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")

        # imputation par la médiane, mais si tout est NaN → médiane = NaN, donc on ne triche pas
        df[num_cols] = df[num_cols].fillna(df[num_cols].median())

    # 3) colonnes non numériques : on impute seulement les champs de contexte
    #    (on évite de toucher à des colonnes où NaN a un vrai sens métier)
    non_numeric_cols = df.select_dtypes(exclude=["number"]).columns.tolist()

    # On peut décider que ces colonnes peuvent être remplies par "Unknown"
    safe_text_cols = [
        "pays_origine",
        "fabricant",
        "pays_commercialisation",
        "distributeur",
    ]

    for col in safe_text_cols:
        if col in non_numeric_cols:
            df[col] = df[col].fillna("Unknown")

    # 4) on rattache le df modifié au processor pour utiliser ses méthodes
    processor.df = df

    # 5) suppression des colonnes trop vides
    processor.filter_irrelevant_columns(
        methods=["missing_values"],
        max_missing=0.8,   # > 80% de NaN → colonne supprimée
    )

    return processor.df


# ====================================================================
#                           4. MAIN ETL
# ====================================================================

def main() -> None:
    print("=== ETL TRANSFORM – OpenFoodFacts ===")
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------------------------
    # 1. Charger tous les JSONL de scraping
    # --------------------------------------------------------------
    files = find_jsonl_files()
    print(f"Fichiers trouvés ({len(files)}) :")
    for f in files:
        print("  -", f)

    # Concaténer tous les fichiers trouvés
    frames = [pd.read_json(f, lines=True) for f in files]
    df = pd.concat(frames, ignore_index=True)

    print(f"\nLignes brutes : {len(df)}")
    print("Colonnes brutes :", list(df.columns))

    # --------------------------------------------------------------
    # 2. Transformation métier spécifique OpenFoodFacts
    # --------------------------------------------------------------
    df = base_transform(df)
    print(f"\nLignes après base_transform : {len(df)}")
    print("Colonnes après base_transform :", list(df.columns))

    # --------------------------------------------------------------
    # 3. Nettoyage avancé
    # --------------------------------------------------------------
    df_clean = asyncio.run(advanced_cleaning(df))

    print(f"\nLignes finales : {len(df_clean)}")
    print("Colonnes finales :", list(df_clean.columns))
    print("\nAperçu des données :")
    print(df_clean.head())

    # --------------------------------------------------------------
    # 4. Sauvegarde
    # --------------------------------------------------------------
    df_clean.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"\n✅ Fichier transformé sauvegardé dans : {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
