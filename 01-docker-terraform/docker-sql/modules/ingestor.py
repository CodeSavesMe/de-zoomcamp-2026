# 01-docker-terraform/docker-sql/modules/ingestor.py

import os
import gzip
import io
import re
import csv
from time import time
from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

import pyarrow.parquet as pq
import pyarrow as pa

from .utils import download_file, fix_datetime_columns

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sanitize_ident(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name


def _qident(name: str) -> str:
    name = _sanitize_ident(name)
    return f'"{name}"'


class TaxiIngestor:
    """
    Hybrid, Stable-mode ingestion:

    - Stable schema:
      - if final exists: staging created as LIKE final INCLUDING ALL
      - if final missing:
          - CSV: bootstrap final schema once from sample (+datetime fix)
          - Parquet: bootstrap final schema once from parquet footer schema

    - Load:
      - CSV/CSV.GZ: COPY from file
      - Parquet: stream batches -> write CSV in-memory -> COPY

    - Safety:
      advisory lock -> staging -> validate -> atomic swap -> ANALYZE
    """

    def __init__(self, user: str, password: str, host: str, port: str, db: str):
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        self.engine = create_engine(conn_str)
        logger.success(f"Database engine created for {host}:{port}/{db}")

    # ---------- meta ----------
    def _table_exists(self, table_name: str) -> bool:
        table_name = _sanitize_ident(table_name)
        with self.engine.connect() as conn:
            return bool(
                conn.execute(
                    text(
                        """
                        SELECT EXISTS (
                          SELECT 1
                          FROM information_schema.tables
                          WHERE table_schema='public' AND table_name=:t
                        )
                        """
                    ),
                    {"t": table_name},
                ).scalar_one()
            )

    def _get_table_columns(self, table_name: str) -> List[str]:
        """
        Return columns in ordinal_position order (important for COPY column alignment).
        """
        table_name = _sanitize_ident(table_name)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:t
                    ORDER BY ordinal_position
                    """
                ),
                {"t": table_name},
            ).fetchall()
        return [r[0] for r in rows]

    def _advisory_lock(self, lock_key: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_lock(hashtext(:k)::bigint)"), {"k": lock_key})

    def _advisory_unlock(self, lock_key: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("SELECT pg_advisory_unlock(hashtext(:k)::bigint)"), {"k": lock_key})

    # ---------- schema bootstrap (CSV) ----------
    def _bootstrap_final_schema_from_csv_sample(
        self, file_path: str, final_table: str, sample_rows: int = 2000
    ) -> None:
        final_table = _sanitize_ident(final_table)
        logger.info(f"Bootstrapping schema for <green>{final_table}</green> from CSV sample rows={sample_rows}")

        df = pd.read_csv(file_path, nrows=sample_rows, compression="infer", low_memory=False)
        df = fix_datetime_columns(df)

        df.head(0).to_sql(name=final_table, con=self.engine, if_exists="replace", index=False)
        logger.info(f"Schema bootstrapped for <green>{final_table}</green>")

    # ---------- schema bootstrap (Parquet) ----------
    def _arrow_type_to_pg(self, arrow_type: pa.DataType) -> str:
        """
        Reasonable mapping for common types.
        Fallback TEXT for complex types.
        """
        t = str(arrow_type)

        # timestamps
        if pa.types.is_timestamp(arrow_type):
            # if timezone is set, prefer TIMESTAMPTZ
            if getattr(arrow_type, "tz", None):
                return "TIMESTAMPTZ"
            return "TIMESTAMP"

        # ints
        if pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type):
            return "SMALLINT"
        if pa.types.is_int32(arrow_type):
            return "INTEGER"
        if pa.types.is_int64(arrow_type):
            return "BIGINT"

        # floats
        if pa.types.is_float32(arrow_type):
            return "REAL"
        if pa.types.is_float64(arrow_type):
            return "DOUBLE PRECISION"

        # bool
        if pa.types.is_boolean(arrow_type):
            return "BOOLEAN"

        # strings
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return "TEXT"

        # dates
        if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
            return "DATE"

        # decimals
        if pa.types.is_decimal(arrow_type):
            # NUMERIC(p,s)
            return f"NUMERIC({arrow_type.precision},{arrow_type.scale})"

        # binary
        if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return "BYTEA"

        # fallback
        logger.debug(f"Arrow type fallback to TEXT: {t}")
        return "TEXT"

    def _bootstrap_final_schema_from_parquet_footer(self, file_path: str, final_table: str) -> None:
        """
        Read parquet footer schema only (fast) and create final table DDL.
        """
        final_table = _sanitize_ident(final_table)
        logger.info(f"Bootstrapping schema for <green>{final_table}</green> from Parquet footer")

        pf = pq.ParquetFile(file_path)
        schema = pf.schema_arrow

        cols_sql = []
        for field in schema:
            col = _sanitize_ident(field.name)
            pg_type = self._arrow_type_to_pg(field.type)
            cols_sql.append(f"{_qident(col)} {pg_type}")

        ddl = f"CREATE TABLE {_qident(final_table)} ({', '.join(cols_sql)})"

        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {_qident(final_table)}"))
            conn.execute(text(ddl))

        logger.info(f"Schema bootstrapped for <green>{final_table}</green>")

    # ---------- staging ----------
    def _recreate_staging_like_final(self, final_table: str, staging_table: str) -> None:
        final_table = _sanitize_ident(final_table)
        staging_table = _sanitize_ident(staging_table)

        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {_qident(staging_table)}"))
            conn.execute(text(f"CREATE TABLE {_qident(staging_table)} (LIKE {_qident(final_table)} INCLUDING ALL)"))

        logger.info(f"Created staging <green>{staging_table}</green> LIKE <green>{final_table}</green>")

    # ---------- COPY CSV ----------
    def _copy_csv_to_table(self, file_path: str, table_name: str) -> None:
        table_name = _sanitize_ident(table_name)
        copy_sql = f"COPY {_qident(table_name)} FROM STDIN WITH (FORMAT csv, HEADER true)"

        logger.info(f"Starting COPY (CSV) into <green>{table_name}</green> ...")
        raw_conn = self.engine.raw_connection()
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

    # ---------- COPY Parquet (stream batches) ----------
    def _copy_parquet_to_table(
        self,
        file_path: str,
        table_name: str,
        batch_size: int = 100_000,
    ) -> None:
        """
        Stream parquet batches -> write CSV in memory -> COPY into Postgres.
        Stable-mode requirement: column order must match target table columns.
        """
        table_name = _sanitize_ident(table_name)
        target_cols = self._get_table_columns(table_name)
        if not target_cols:
            raise RuntimeError(f"Target table {table_name} has no columns")

        col_list_sql = ", ".join(_qident(c) for c in target_cols)
        copy_sql = f"COPY {_qident(table_name)} ({col_list_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)"

        logger.info(f"Starting COPY (Parquet stream) into <green>{table_name}</green> ...")
        pf = pq.ParquetFile(file_path)

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                for i, batch in enumerate(pf.iter_batches(batch_size=batch_size)):
                    # Arrow RecordBatch -> Table, then select columns in target order
                    tbl = pa.Table.from_batches([batch])

                    # ensure all required cols exist
                    missing = [c for c in target_cols if c not in tbl.column_names]
                    if missing:
                        raise RuntimeError(f"Parquet missing columns required by target table: {missing}")

                    tbl = tbl.select(target_cols)

                    # Convert to pandas just for CSV serialization (robust & simple)
                    df = tbl.to_pandas()
                    df = fix_datetime_columns(df)

                    buf = io.StringIO()
                    # no header; CSV format; keep empty for NULLs (COPY CSV treats empty as NULL)
                    df.to_csv(
                        buf,
                        index=False,
                        header=False,
                        quoting=csv.QUOTE_MINIMAL,
                    )
                    buf.seek(0)

                    cur.copy_expert(copy_sql, buf)

                    if (i + 1) % 10 == 0:
                        logger.debug(f"Streamed {i+1} parquet batches...")

            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

        logger.info(f"COPY finished for <green>{table_name}</green> (Parquet stream)")

    # ---------- validation ----------
    def _detect_datetime_column(self, table_name: str) -> Optional[str]:
        table_name = _sanitize_ident(table_name)
        candidates = ["lpep_pickup_datetime", "tpep_pickup_datetime", "pickup_datetime"]

        cols = set(self._get_table_columns(table_name))
        for c in candidates:
            if c in cols:
                return c
        return None

    def _infer_expected_month_from_table(self, table_name: str) -> Optional[str]:
        m = re.search(r"(\d{4})_(\d{2})", table_name)
        if m:
            return f"{m.group(1)}_{m.group(2)}"
        return None

    def _validate_staging(self, staging_table: str, expected_month: Optional[str] = None) -> Dict[str, Any]:
        staging_table = _sanitize_ident(staging_table)
        dt_col = self._detect_datetime_column(staging_table)

        with self.engine.connect() as conn:
            rowcount = conn.execute(text(f"SELECT COUNT(*) FROM {_qident(staging_table)}")).scalar_one()

            result: Dict[str, Any] = {"rowcount": rowcount, "datetime_col": dt_col}

            if dt_col:
                r = conn.execute(
                    text(
                        f"""
                        SELECT
                          MIN({_qident(dt_col)}) AS min_dt,
                          MAX({_qident(dt_col)}) AS max_dt,
                          COUNT(*) FILTER (WHERE {_qident(dt_col)} IS NULL) AS null_dt
                        FROM {_qident(staging_table)}
                        """
                    )
                ).mappings().one()
                result.update({"min_dt": r["min_dt"], "max_dt": r["max_dt"], "null_dt": r["null_dt"]})

        logger.info(f"Validation result: {result}")

        if rowcount == 0:
            raise RuntimeError("Validation failed: staging table has 0 rows")

        # warn-only month check
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

    # ---------- swap & optimize ----------
    def _swap_tables_atomically(self, final_table: str, staging_table: str) -> None:
        final_table = _sanitize_ident(final_table)
        staging_table = _sanitize_ident(staging_table)
        backup_table = _sanitize_ident(f"{final_table}__backup")

        logger.info(
            f"Swapping atomically: staging=<green>{staging_table}</green> -> final=<green>{final_table}</green>"
        )

        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {_qident(backup_table)}"))

            conn.execute(
                text(
                    f"""
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema='public' AND table_name='{final_table}'
                        ) THEN
                            EXECUTE 'ALTER TABLE {_qident(final_table)} RENAME TO {backup_table}';
                        END IF;
                    END $$;
                    """
                )
            )

            conn.execute(text(f"ALTER TABLE {_qident(staging_table)} RENAME TO {final_table}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {_qident(backup_table)}"))

        logger.success(f"Swap completed: <green>{final_table}</green> updated")

    def _post_load_optimize(self, table_name: str) -> None:
        table_name = _sanitize_ident(table_name)
        with self.engine.begin() as conn:
            conn.execute(text(f"ANALYZE {_qident(table_name)}"))
        logger.info(f"ANALYZE completed for <green>{table_name}</green>")

    # ---------- public ----------
    def run(
        self,
        url: str,
        table_name: str,
        keep_local: bool = True,
        parquet_batch_size: int = 100_000,
    ) -> None:
        table_name = _sanitize_ident(table_name)
        staging_table = _sanitize_ident(f"{table_name}__staging")
        lock_key = f"ingest:{table_name}"

        file_path = download_file(url)
        start = time()

        try:
            self._advisory_lock(lock_key)

            is_parquet = file_path.endswith(".parquet")

            # ensure final schema exists (stable)
            if not self._table_exists(table_name):
                if is_parquet:
                    self._bootstrap_final_schema_from_parquet_footer(file_path, table_name)
                else:
                    self._bootstrap_final_schema_from_csv_sample(file_path, table_name, sample_rows=2000)

            # staging always LIKE final
            self._recreate_staging_like_final(table_name, staging_table)

            # load staging
            if is_parquet:
                self._copy_parquet_to_table(file_path, staging_table, batch_size=parquet_batch_size)
            else:
                self._copy_csv_to_table(file_path, staging_table)

            # validate
            expected_month = self._infer_expected_month_from_table(table_name)
            self._validate_staging(staging_table, expected_month=expected_month)

            # swap
            self._swap_tables_atomically(table_name, staging_table)
            self._post_load_optimize(table_name)

            logger.success(f"Ingestion complete in <green>{time() - start:.2f}</green>s")

        except Exception as e:
            logger.exception(f"Ingestion failed: {e}")
            # cleanup staging best-effort
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {_qident(staging_table)}"))
            except Exception:
                logger.warning("Failed to drop staging table during cleanup.")
            raise

        finally:
            try:
                self._advisory_unlock(lock_key)
            except Exception:
                logger.warning("Failed to release advisory lock (it may already be released).")

            if (not keep_local) and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    logger.warning("Failed to remove local file during cleanup.")
