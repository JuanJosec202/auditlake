from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    jdbc_url: str
    user: str
    password: str
    batch_size: int = 1000
    delete_chunk: int = 4000
    fail_after_iceberg_commit: bool = False
    events_table: str = "bronze.audit_events"
    runs_table: str = "bronze.ingestion_runs"
    # Hint de la lectura de la outbox (fase 6 compara variantes con y sin RCSI). Valores permitidos:
    # "READPAST" (línea base), "READCOMMITTEDLOCK, READPAST" o "" (sin hint).
    read_hint: str = "READPAST"

    @staticmethod
    def from_env() -> Settings:
        return Settings(
            jdbc_url=os.environ["OUTBOX_JDBC_URL"],
            user=os.environ["OUTBOX_USER"],
            password=os.environ["OUTBOX_PASSWORD"],
            batch_size=int(os.environ.get("OUTBOX_BATCH_SIZE", "1000")),
            fail_after_iceberg_commit=os.environ.get("FAIL_AFTER_ICEBERG_COMMIT") == "1",
            read_hint=os.environ.get("OUTBOX_READ_HINT", "READPAST"),
        )
