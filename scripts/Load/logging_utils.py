from __future__ import annotations

from datetime import datetime
from functools import wraps
from typing import Callable, TypeVar, ParamSpec

P = ParamSpec("P")
R = TypeVar("R")


def log_action(action: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Décorateur de log d'exécution.

    Ce module sert à rendre la partie LOAD robuste :
    - Si le projet expose déjà un décorateur `log_action` (dans scripts/data_utils.py),
      on l'utilise.
    - Sinon, on utilise un fallback local qui affiche le début/fin de l'action et la durée.

    Args:
        action: Libellé humain de l'action (ex: "Saving CSV file").

    Returns:
        Un décorateur qui enveloppe une fonction et affiche des logs.
    """
    try:
        from ..data_utils import log_action as project_log_action  # type: ignore
        return project_log_action(action)
    except Exception:
        def decorator(func: Callable[P, R]) -> Callable[P, R]:
            @wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                start = datetime.now()
                print(f"[INFO {start}] - {action}")
                result = func(*args, **kwargs)
                end = datetime.now()
                print(f"[INFO {end}] - {action} terminé en {(end - start).total_seconds():.3f}s")
                return result
            return wrapper
        return decorator
