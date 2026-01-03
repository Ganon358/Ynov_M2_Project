from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import create_engine

from .base_loader import BaseLoader
from ..data_utils import log_action


class SQLLoader(BaseLoader):
    def __init__(
        self,
        connection_url: str,
        table_name: str,
        if_exists: str = "replace",  # 'append', 'replace', 'fail'
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.engine = create_engine(connection_url)
        self.table_name = table_name
        self.if_exists = if_exists

    @log_action("Loading data into SQL database")
    def _load_impl(
        self,
        df: pd.DataFrame,
        chunksize: Optional[int] = 10_000,
        **_,
    ):
        df.to_sql(
            self.table_name,
            self.engine,
            if_exists=self.if_exists,
            index=False,
            chunksize=chunksize,
        )
        print(f"Données chargées dans la table SQL '{self.table_name}'")
        # Pas de "Path" réel pour une DB, on renvoie juste une chaîne descriptive
        return self.output_dir / f"{self.table_name}.sql_loaded"
