# 01-docker-terraform/docker-sql/modules/utils.py

import os
import sys
from urllib.parse import urlparse
import pandas as pd
from loguru import logger
import requests


# ---------- Paths ----------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Data dir for downloaded files (configurable via .env)
DATA_DIR = os.getenv("DATA_DIR", os.path.join(BASE_DIR, "data"))
os.makedirs(DATA_DIR, exist_ok=True)

# Log dir
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ---------- Logger configuration ----------
logger.remove()

# Log to console 
logger.add(
    sys.stdout,
    level=os.getenv("LOG_LEVEL", "INFO"),
    colorize=True,
    backtrace=False,
    diagnose=False,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
           "<level>{level}</level> | "
           "<level>{message}</level>",
)

# Log to file (detailed)
logger.add(
    os.path.join(LOG_DIR, "app.log"),
    rotation="10 MB",
    retention="7 days",
    level="DEBUG",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
)


def download_file(url: str) -> str:
    """
    Download a file from the given URL to DATA_DIR and return the local file path.
    Uses requests streaming to avoid relying on wget and to be safer than os.system.
    """
    parsed = urlparse(url)
    file_name = os.path.basename(parsed.path)

    if not file_name:
        raise ValueError("URL is invalid or file name could not be determined!")

    local_path = os.path.join(DATA_DIR, file_name)

    if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
        logger.info(f"File already exists: {local_path}, skipping download.")
        return local_path

    logger.info(f"Downloading data from {url} -> {local_path}")

    # Stream download
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1MB
                if chunk:
                    f.write(chunk)

    if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
        raise RuntimeError("Download completed but file is missing/empty!")

    return local_path


def fix_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert pickup and dropoff columns to datetime (for NYC Taxi datasets).
    """
    datetime_columns = [
        ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
        ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
    ]

    for pickup, dropoff in datetime_columns:
        if pickup in df.columns and dropoff in df.columns:
            logger.debug(f"Converting columns {pickup} and {dropoff} to datetime")
            df[pickup] = pd.to_datetime(df[pickup], errors="coerce")
            df[dropoff] = pd.to_datetime(df[dropoff], errors="coerce")

    return df
