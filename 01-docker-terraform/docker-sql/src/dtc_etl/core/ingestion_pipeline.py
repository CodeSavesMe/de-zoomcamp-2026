# 01-docker-terraform/docker-sql/src/dtc_etl/core/ingestion_pipeline.py

from __future__ import annotations

import os
from dataclasses import dataclass
from time import time

from loguru import logger
from sqlalchemy import text

from dtc_etl.ports.database import Database
from dtc_etl.ports.lock import LockManager
from dtc_etl.ports.schema import SchemaManager
from dtc_etl.ports.loader import Loader
from dtc_etl.ports.validator import Validator
from dtc_etl.ports.swapper import Swapper

from dtc_etl.utils.downloader import download_file
from dtc_etl.utils.file_types import detect_file_format, FileFormat
from dtc_etl.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class IngestionPipeline:
    db: Database
    lock: LockManager
    schema: SchemaManager
    csv_loader: Loader
    parquet_loader: Loader
    validator: Validator
    swapper: Swapper
    optimizer: object  # PostLoadOptimizer

    def run(self, url: str, table_name: str, keep_local: bool = True) -> None:
        table_name = sanitize_ident(table_name)
        staging_table = sanitize_ident(f"{table_name}__staging")
        lock_key = f"ingest:{table_name}"

        file_path = download_file(url)
        fmt = detect_file_format(file_path)

        start = time()

        try:
            with self.lock.acquire(lock_key):
                # stable schema ensure
                if not self.db.table_exists(table_name):
                    self.schema.ensure_final_schema(file_path=file_path, final_table=table_name)

                # staging like final
                self.schema.recreate_staging_like_final(final_table=table_name, staging_table=staging_table)

                # load staging
                if fmt == FileFormat.PARQUET:
                    self.parquet_loader.load(file_path=file_path, table_name=staging_table)
                elif fmt == FileFormat.CSV:
                    self.csv_loader.load(file_path=file_path, table_name=staging_table)
                elif fmt == FileFormat.TSV:
                    # NOTE: TSV needs a different COPY delimiter. Add a TsvCopyLoader if needed.
                    raise ValueError(f"TSV not supported yet (needs tab delimiter COPY): {file_path}")
                else:
                    raise ValueError(f"Unsupported file format: {file_path} ({fmt})")

                # validate
                expected_month = self.validator.infer_expected_month_from_table(table_name)
                self.validator.validate_staging(staging_table, expected_month=expected_month)

                # swap + analyze
                self.swapper.swap_tables_atomically(final_table=table_name, staging_table=staging_table)
                self.optimizer.analyze(table_name)  # type: ignore[attr-defined]

            logger.success(f"Ingestion complete in <green>{time() - start:.2f}</green>s")

        except Exception as e:
            logger.exception(f"Ingestion failed: {e}")
            try:
                with self.db.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {qident(staging_table)}"))
            except Exception:
                logger.warning("Failed to drop staging table during cleanup.")
            raise

        finally:
            if (not keep_local) and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    logger.warning("Failed to remove local file during cleanup.")
