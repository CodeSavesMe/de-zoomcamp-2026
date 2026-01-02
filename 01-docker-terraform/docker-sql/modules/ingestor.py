# 01-docker-terraform/docker-sql/modules/ingestor.py

import pandas as pd
from sqlalchemy import create_engine
from loguru import logger
from time import time
from .utils import download_file, fix_datetime_columns


class TaxiIngestor:
    def __init__(self, user: str, password: str, host: str, port: str, db: str):
        """
        Initialize Postgres connection.
        """
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        self.engine = create_engine(conn_str)
        logger.success(f"Database engine created for {host}:{port}/{db}")

    def run(self, url: str, table_name: str, chunksize: int = 100_000):
        """
        Ingest CSV or Parquet file into Postgres using chunks.
        """
        file_path = download_file(url)
        start_time = time()
        logger.info(f"Starting ingestion process for table: <green>{table_name}</green>")
        logger.debug(f"Local file path: {file_path}")

        try:
            # ----------------- Parquet ingestion -----------------
            if file_path.endswith(".parquet"):
                df = pd.read_parquet(file_path)
                df = fix_datetime_columns(df)

                # Create/replace table schema
                df.head(0).to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists="replace",
                    index=False,
                )

                # Insert all data
                df.to_sql(
                    name=table_name,
                    con=self.engine,
                    if_exists="append",
                    index=False,              # IMPORTANT: avoid index column
                    chunksize=chunksize,
                    method="multi",           # faster inserts
                )

                duration = time() - start_time
                logger.info(
                    f"Ingestion completed for table: <green>{table_name}</green>, "
                    f"<green>{len(df)}</green> records ingested in <green>{duration:.2f}</green> seconds"
                )

            # ----------------- CSV / CSV.GZ ingestion -----------------
            else:
                reader = pd.read_csv(
                    file_path,
                    iterator=True,
                    chunksize=chunksize,
                    compression="infer",  # handles .gz automatically
                    low_memory=False,
                )

                first_chunk = True
                total_rows = 0

                for chunk in reader:
                    chunk = fix_datetime_columns(chunk)

                    mode = "replace" if first_chunk else "append"
                    chunk.to_sql(
                        name=table_name,
                        con=self.engine,
                        if_exists=mode,
                        index=False,
                        chunksize=chunksize,
                        method="multi",
                    )

                    first_chunk = False
                    total_rows += len(chunk)
                    logger.info(f"Inserted chunk of {len(chunk)} rows into <green>{table_name}</green>")

                duration = time() - start_time
                logger.info(
                    f"Total ingestion completed: <green>{total_rows}</green> rows "
                    f"into <green>{table_name}</green> in <green>{duration:.2f}</green> seconds"
                )

        except Exception as e:
            logger.exception(f"An error occurred during ingestion: {e}")
            raise
