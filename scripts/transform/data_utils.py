from datetime import datetime
from functools import wraps
from typing import Callable, Optional

import pandas as pd
import numpy as np


def log_action(action: str) -> Callable:
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            print(f"[INFO {start_time}] - {action}")
            result = func(*args, **kwargs)
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            if hasattr(wrapper, 'log'):
                wrapper.log.append({
                    "action": action,
                    "duration": duration,
                    "timestamp": start_time
                })
            return result
        return wrapper
    return decorator


def verify_column_exists(df: pd.DataFrame, col_name: str) -> None:
    if col_name not in df.columns:
        import difflib
        matches = difflib.get_close_matches(col_name, df.columns, n=5, cutoff=0.4)
        raise ValueError(
            f"Colonne '{col_name}' introuvable. Suggestions proches : {matches}"
        )


def load_data(file_path: str, limit: Optional[int] = None) -> pd.DataFrame:
    """
    Charge un fichier CSV ou JSONL.

    - Si le fichier se termine par .jsonl → lecture ligne par ligne.
    - Sinon → CSV classique.
    - limit permet de ne charger que les N premières lignes.
    """
    try:
        if file_path.endswith(".jsonl"):
            df = pd.read_json(file_path, lines=True, nrows=limit)
        else:
            df = pd.read_csv(file_path, sep=",", encoding="utf-8", nrows=limit)
        return df
    except Exception as e:
        print(f"Erreur lors du chargement du fichier '{file_path}' : {e}")
        raise


def get_numeric_columns(df: pd.DataFrame) -> list:
    return df.select_dtypes(include=[np.number]).columns.tolist()


def get_categorical_columns(df: pd.DataFrame) -> list:
    return df.select_dtypes(include=['object', 'category']).columns.tolist()


def get_datetime_columns(df: pd.DataFrame) -> list:
    return df.select_dtypes(include=['datetime']).columns.tolist()


def get_ordinal_columns(df: pd.DataFrame) -> list:
    return [col for col in get_categorical_columns(df) if df[col].nunique() <= 10]


def get_nominal_columns(df: pd.DataFrame) -> list:
    return [col for col in get_categorical_columns(df) if df[col].nunique() > 10]
