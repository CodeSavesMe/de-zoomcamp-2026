# 01-docker-terraform/src/docker-sql/dtc_etl/ports/swapper.py

from __future__ import annotations

from typing import Protocol


class Swapper(Protocol):
    def swap_tables_atomically(self, final_table: str, staging_table: str) -> None: ...


