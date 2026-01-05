# 01-docker-terraform/src/docker-sql/dtc_etl/db/lock.py

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass

from loguru import logger
from sqlalchemy import text

from dtc_etl.ports.database import Database
from dtc_etl.ports.lock import LockManager

@dataclass(frozen=True)
class AdvisoryLock:
    db: Database

    @contextmanager
    def acquire(self, lock_key: str):
        with self.db.begin() as conn:
            conn.execute(
                text("SELECT pg_advisory_lock(hashtext(:k)::bigint)"), {"k": lock_key}
            )
        try:
            yield
        finally:
            try:
                with self.db.begin() as conn:
                    conn.execute(
                        text("SELECT pg_advisory_unlock(hashtext(:k)::bigint)"), {"k": lock_key}
                    )
            except Exception:
                logger.warning(f"Failed to release advisory lock: {lock_key}, it may already released")