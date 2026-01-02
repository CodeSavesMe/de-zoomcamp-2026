# 01-docker-terraform/docker-sql/main.py

import os
import argparse
from loguru import logger
from dotenv import load_dotenv
from modules.ingestor import TaxiIngestor

# Load environment variables from .env
load_dotenv()

def main():
    parser = argparse.ArgumentParser(description='Modular Data Ingestion with .env support')

    # Database configuration from .env
    parser.add_argument('--user', default=os.getenv('DB_USER', 'postgres'), help='Postgres username')
    parser.add_argument('--password', default=os.getenv('DB_PASSWORD', 'postgres'), help='Postgres password')
    parser.add_argument('--host', default=os.getenv('DB_HOST', 'localhost'), help='Postgres host')
    parser.add_argument('--port', default=os.getenv('DB_PORT', '5432'), help='Postgres port')
    parser.add_argument('--db', default=os.getenv('DB_NAME', 'ny_taxi'), help='Postgres database name')
    
    # Data URL from .env
    parser.add_argument('--url', default=os.getenv('DATA_URL'), help='URL of the data file')
    parser.add_argument('--table_name', required=True, help='Name of the destination table')

    args = parser.parse_args()

    # Validate URL
    if not args.url:
        logger.error("DATA_URL not found in .env or command-line arguments!")
        return

    logger.info(f"Starting ingestion for table <green>{args.table_name}</green>")
    logger.debug(f"Source URL: {args.url}")

    # Initialize ingestor
    ingestor = TaxiIngestor(
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
        db=args.db
    )

    try:
        ingestor.run(url=args.url, table_name=args.table_name)
        logger.success(f"Successfully ingested <green>{args.table_name}</green>")
    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")

if __name__ == '__main__':
    main()
