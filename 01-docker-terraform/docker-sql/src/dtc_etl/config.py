# 01-docker-terraform/docker-sql/dtc_etl/config.py

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from loguru import logger


def project_root() -> Path:
    """
    docker-sql/src/dtc_etl/config.py -> parents:
    dtc_etl (0), src (1), docker-sql (2), 01-docker-terraform (3)
    """
    return Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class Paths:
    base_dir: Path
    data_dir: Path
    log_dir: Path


def build_paths() -> Paths:
    base = project_root()

    data_dir = Path(os.getenv("DATA_DIR", str(base / "data")))
    data_dir.mkdir(parents=True, exist_ok=True)

    log_dir = base / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    return Paths(base_dir=base, data_dir=data_dir, log_dir=log_dir)


def configure_logging(paths: Paths) -> None:

    logger.remove()

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

    logger.add(
        str(paths.log_dir / "app.log"),
        rotation="10 MB",
        retention="7 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}",
    )
