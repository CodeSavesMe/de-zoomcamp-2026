# 01-docker-terraform/src/docker-sql/dtc_etl/ports/lock.py

from __future__ import annotations

from typing import Protocol, ContextManager


class LockManager(Protocol):
    def acquire(self, lock_key: str) -> ContextManager[None]: ...
