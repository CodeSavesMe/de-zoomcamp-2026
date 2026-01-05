# 01-docker-terraform/docker-sql/src/dtc_etl/db/validator_repo.py

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

from loguru import logger
from sqlalchemy import text

from dtc_etl.ports.database import Database
from dtc_etl.ports.validator import Validator
from dtc_etl.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class PostgresStagingValidator(Validator):
    db: Database

    def _detect_datetime_column(self, table_name: str) -> Optional[str]:

        table_name = sanitize_ident(table_name)

        # candidate pickup columns
        candidates = [
            "lpep_pickup_datetime",
            "tpep_pickup_datetime",
            "pickup_datetime",
        ]

        columns = self.db.get_table_columns(table_name)

        norm_map = {c.strip(). lower(): c for c in columns}

        for cand in candidates:
            key = cand.strip().lower()
            if key in norm_map:
                return norm_map[key]

        #debug when not found
        logger.debug(f"Datetime column not found for {table_name}. Columns: {columns}")
        return None
    
    def infer_expected_month_from_table(self, table_name: str) -> Optional[str]:
        m = re.search(r"(\d{4})_(\d{2})", table_name)
        if m:
            return f"{m.group(1)}_{m.group(2)}"
        return None

    def validate_staging(self, staging_table: str, expected_month: Optional[str] = None) -> Dict[str, Any]:
        staging_table = sanitize_ident(staging_table)
        dt_col = self._detect_datetime_column(staging_table)

        with self.db.connect() as conn:
            rowcount = conn.execute(text(f"SELECT COUNT(*) FROM {qident(staging_table)}")).scalar_one()

            result: Dict[str, Any] = {"rowcount": rowcount, "datetime_col": dt_col}

            if dt_col:
                r = conn.execute(
                    text(
                        f"""
                        SELECT
                          MIN({qident(dt_col)}) AS min_dt,
                          MAX({qident(dt_col)}) AS max_dt,
                          COUNT(*) FILTER (WHERE {qident(dt_col)} IS NULL) AS null_dt
                        FROM {qident(staging_table)}
                        """
                    )
                ).mappings().one()
                result.update({"min_dt": r["min_dt"], "max_dt": r["max_dt"], "null_dt": r["null_dt"]})

        logger.info(f"Validation result: {result}")

        if rowcount == 0:
            raise RuntimeError("Validation failed: staging table has 0 rows")

        if expected_month and dt_col and result.get("min_dt") and result.get("max_dt"):
            try:
                yyyy, mm = expected_month.split("_")
                start = datetime(int(yyyy), int(mm), 1)
                end = datetime(int(yyyy) + 1, 1, 1) if int(mm) == 12 else datetime(int(yyyy), int(mm) + 1, 1)

                if not (start <= result["min_dt"] < end and start <= result["max_dt"] < end):
                    logger.warning(
                        f"Datetime range outside expected month {expected_month}: "
                        f"min={result['min_dt']}, max={result['max_dt']}"
                    )
            except Exception:
                logger.debug("Month validation skipped (parse failed).")

        return result
