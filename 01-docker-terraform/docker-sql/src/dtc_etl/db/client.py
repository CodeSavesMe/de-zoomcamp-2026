# 01-docker-terraform/src/docker-sql/dtc_etl/db/client.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, ContextManager


from sqlalchemy import create_engine,text

from dtc_etl.utils.identifiers import sanitize_ident

@dataclass(frozen=True)
class PostgresClient:
    engine: Engine

    @classmethod
    def from_params(
            cls,
            user: str,
            password: str,
            host: str,
            port: str,
            db: str,
    ) -> "PostgreClient":
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        return cls(engine=create_engine(conn_str, pool_pre_ping=True))

    def table_exists(self, table_name: str) -> bool:
        table_name = sanitize_ident(table_name)
        with self.engine.connect() as conn:
            return bool(
            conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_name = 'public' and table_name=:t
                    )
                    """
                ),
                {"t": table_name},
            ).scalar_one()
        )

    def get_table_columns(self, table_name: str) -> List[str]:
        table_name = sanitize_ident(table_name)
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'public' and table_name=:t
                    ORDER BY ordinal_position
                    """
                ),
                {"t": table_name},
            ).fetchall()
        return [r[0] for r in rows]

    def raw_connection(self) -> Any:
        return self.engine.raw_connection()

    def begin(self) -> ContextManager[Any]:
        return self.engine.begin()

    def connect(self) -> ContextManager[Any]:
        return self.engine.connect()

