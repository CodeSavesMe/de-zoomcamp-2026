# 01-docker-terraform/docker-sql/src/dtc_etl/db/loader_parquet.py

from __future__ import annotations

import csv
import io
from dataclasses import dataclass

from loguru import logger
import pyarrow.parquet as pq
import pyarrow as pa

from dtc_etl.ports.database import Database
from dtc_etl.ports.loader import Loader
from dtc_etl.utils.identifiers import sanitize_ident, qident
from dtc_etl.utils.datetime_fix import fix_datetime_columns


@dataclass(frozen=True)
class ParquetStreamLoader(Loader):
    db: Database
    batch_size: int = 100_000

    def load(self, file_path: str, table_name: str) -> None:
        table_name = sanitize_ident(table_name)

        target_cols = self.db.get_table_columns(table_name)
        if not target_cols:
            raise RuntimeError(f"Target table {table_name} has no columns")

        col_list_sql = ", ".join(qident(c) for c in target_cols)
        copy_sql = f"COPY {qident(table_name)} ({col_list_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)"

        logger.info(f"Starting COPY (Parquet stream) into <green>{table_name}</green> ...")
        pf = pq.ParquetFile(file_path)

        raw_conn = self.db.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                for i, batch in enumerate(pf.iter_batches(batch_size=self.batch_size)):
                    tbl = pa.Table.from_batches([batch])

                    missing = [c for c in target_cols if c not in tbl.column_names]
                    if missing:
                        raise RuntimeError(f"Parquet missing columns required by target table: {missing}")

                    tbl = tbl.select(target_cols)

                    df = tbl.to_pandas()
                    df = fix_datetime_columns(df)

                    buf = io.StringIO()
                    df.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
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
