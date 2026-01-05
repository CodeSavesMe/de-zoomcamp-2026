# 01-docker-terraform/docker-sql/src/dtc_etl/db/swapper.py

from __future__ import annotations

from dataclasses import dataclass

from loguru import logger
from sqlalchemy import text

from dtc_etl.ports.database import Database
from dtc_etl.ports.swapper import Swapper
from dtc_etl.utils.identifiers import sanitize_ident, qident


@dataclass(frozen=True)
class AtomicSwapper(Swapper):
    db: Database

    def swap_tables_atomically(self, final_table: str, staging_table: str) -> None:
        final_table = sanitize_ident(final_table)
        staging_table = sanitize_ident(staging_table)
        backup_table = sanitize_ident(f"{final_table}__backup")

        logger.info(
            f"Swapping atomically: staging=<green>{staging_table}</green> -> final=<green>{final_table}</green>"
        )

        with self.db.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {qident(backup_table)}"))

            conn.execute(
                text(
                    f"""
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema='public' AND table_name='{final_table}'
                        ) THEN
                            EXECUTE 'ALTER TABLE {qident(final_table)} RENAME TO {backup_table}';
                        END IF;
                    END $$;
                    """
                )
            )

            conn.execute(text(f"ALTER TABLE {qident(staging_table)} RENAME TO {final_table}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {qident(backup_table)}"))

        logger.success(f"Swap completed: <green>{final_table}</green> updated")
