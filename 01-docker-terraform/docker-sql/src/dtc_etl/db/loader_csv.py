# 01-docker-terraform/docker-sql/src/dtc_etl/db/loader_csv.py

from __future__ import annotations

import gzip
from dataclasses import dataclass

from loguru import logger

from dtc_etl.ports.database import Database
from dtc_etl.ports.loader import Loader
from dtc_etl.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class CsvCopyLoader(Loader):
    db: Database

    def load(self, file_path: str, table_name: str) -> None:
        table_name = sanitize_ident(table_name)
        copy_sql = f"COPY {qident(table_name)} FROM STDIN WITH (FORMAT csv, HEADER true)"

        logger.info(f"Starting COPY (CSV) into <green>{table_name}</green> ...")
        raw_conn = self.db.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                opener = gzip.open if file_path.endswith(".gz") else open
                with opener(file_path, "rt") as f:
                    cur.copy_expert(copy_sql, f)
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

        logger.info(f"COPY finished for <green>{table_name}</green>")
