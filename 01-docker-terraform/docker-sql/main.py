# 01-docker-terraform/docker-sql/main.py

from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env (01-docker-terraform/.env)
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

import os
import re
from urllib.parse import urlparse
import argparse
from loguru import logger

from dtc_etl.config import build_paths, configure_logging
from dtc_etl.db.client import PostgresClient
from dtc_etl.db.lock import AdvisoryLock
from dtc_etl.db.schema import PostgresSchemaManager
from dtc_etl.db.loader_csv import CsvCopyLoader
from dtc_etl.db.loader_parquet import ParquetStreamLoader
from dtc_etl.db.validator_repo import PostgresStagingValidator
from dtc_etl.db.swapper import AtomicSwapper
from dtc_etl.db.optimize import PostLoadOptimizer
from dtc_etl.core.ingestion_pipeline import IngestionPipeline


def infer_table_name_from_url(url: str) -> str:
    filename = os.path.basename(urlparse(url).path)
    name = re.sub(r"\.csv(\.gz)?$|\.parquet$", "", filename)
    return name.replace("-", "_")


def main():
    # Configure logger (same style as old modules/utils.py)
    paths = build_paths()
    configure_logging(paths)

    parser = argparse.ArgumentParser(description="DTC ETL ingestion with .env support (performance-first)")

    # Database configuration from .env
    parser.add_argument("--user", default=os.getenv("DB_USER", "postgres"), help="Postgres username")
    parser.add_argument("--password", default=os.getenv("DB_PASSWORD", "postgres"), help="Postgres password")
    parser.add_argument("--host", default=os.getenv("DB_HOST", "localhost"), help="Postgres host")
    parser.add_argument("--port", default=os.getenv("DB_PORT", "5432"), help="Postgres port")
    parser.add_argument("--db", default=os.getenv("DB_NAME", "ny_taxi"), help="Postgres database name")

    # Data URL
    parser.add_argument("--url", default=os.getenv("DATA_URL"), help="URL of the data file")
    parser.add_argument("--table_name", required=False, default=None,
                        help="Destination table name (optional: auto from URL if omitted)")

    # Runtime knobs
    parser.add_argument("--keep_local", default=os.getenv("KEEP_LOCAL", "true").lower() == "true")
    parser.add_argument("--parquet_batch_size", type=int, default=int(os.getenv("PARQUET_BATCH_SIZE", "100000")))

    args = parser.parse_args()

    if not args.url:
        logger.error("DATA_URL not found in .env or command-line arguments!")
        return

    if not args.table_name:
        args.table_name = infer_table_name_from_url(args.url)
        logger.info(f"Auto table_name from URL: <green>{args.table_name}</green>")

    logger.info(f"Starting ingestion for table <green>{args.table_name}</green>")
    logger.debug(f"Source URL: {args.url}")

    db = PostgresClient.from_params(
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        db=args.db,
    )

    pipeline = IngestionPipeline(
        db=db,
        lock=AdvisoryLock(db),
        schema=PostgresSchemaManager(db, sample_rows=2000),
        csv_loader=CsvCopyLoader(db),
        parquet_loader=ParquetStreamLoader(db, batch_size=args.parquet_batch_size),
        validator=PostgresStagingValidator(db),
        swapper=AtomicSwapper(db),
        optimizer=PostLoadOptimizer(db),
    )

    pipeline.run(
        url=args.url,
        table_name=args.table_name,
        keep_local=args.keep_local,
    )

    logger.success(f"Successfully ingested <green>{args.table_name}</green>")


if __name__ == "__main__":
    main()
