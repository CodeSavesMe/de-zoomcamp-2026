# 01-docker-terraform/docker-sql/src/dtc_etl/db/optimize.py

from __future__ import annotations

from dataclasses import dataclass

from loguru import logger
from sqlalchemy import text

from dtc_etl.ports.database import Database
from dtc_etl.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class PostLoadOptimizer:
    db: Database

    def analyze(self, table_name: str) -> None:
        table_name = sanitize_ident(table_name)
        with self.db.begin() as conn:
            conn.execute(text(f"ANALYZE {qident(table_name)}"))
        logger.info(f"ANALYZE completed for <green>{table_name}</green>")
