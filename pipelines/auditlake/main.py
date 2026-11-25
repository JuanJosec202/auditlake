"""Una corrida del ingestor: outbox (SQL Server) -> bronze.audit_events (Iceberg) -> DELETE verificado.

Ejecución manual y en serie, sin loop:  docker compose run --rm spark python -m auditlake.main
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession

from auditlake import audit_sink, outbox_source
from auditlake.classify import Outcome, classify, count_outcomes, verified_ids
from auditlake.config import Settings

log = logging.getLogger("auditlake.ingest")


class InjectedFailure(RuntimeError):
    """Caída simulada entre el commit Iceberg y el DELETE en SQL (FAIL_AFTER_ICEBERG_COMMIT=1)."""


@dataclass
class RunResult:
    run_id: str
    read_count: int = 0
    inserted: int = 0
    duplicates: int = 0
    conflicts: int = 0
    deleted: int = 0


def run(spark: SparkSession, s: Settings, run_id: Optional[str] = None) -> RunResult:
    run_id = run_id or str(uuid.uuid4())
    started = datetime.now(timezone.utc)
    result = RunResult(run_id)
    audit_sink.ensure_tables(spark, s)
    try:
        batch = outbox_source.read_batch(spark, s)
        keys = [(r["outbox_id"], r["event_id"], r["payload_hash"]) for r in batch.collect()]
        result.read_count = len(keys)
        log.info("run_id=%s read=%d (ids fijados %s..%s)", run_id, result.read_count,
                 keys[0][0] if keys else "-", keys[-1][0] if keys else "-")

        if keys:
            event_ids = [e for _, e, _ in keys]
            before = audit_sink.archived_hashes(spark, s, event_ids)
            counts = count_outcomes(((e, h) for _, e, h in keys), before)
            new_ids = [e for _, e, h in keys if classify(h, before.get(e)) is Outcome.NEW]
            result.inserted, result.duplicates, result.conflicts = (
                counts.inserted, counts.duplicates, counts.conflicts)

            if new_ids:
                audit_sink.merge_new(spark, s, batch, new_ids, run_id)
            log.info("run_id=%s merge: inserted=%d duplicates=%d conflicts=%d",
                     run_id, result.inserted, result.duplicates, result.conflicts)

            if s.fail_after_iceberg_commit:
                raise InjectedFailure("FAIL_AFTER_ICEBERG_COMMIT=1: caída tras el commit Iceberg, antes del DELETE")

            # Verificación: releer Iceberg y borrar solo los IDs fijados cuyo event_id y hash coinciden.
            after = audit_sink.archived_hashes(spark, s, event_ids)
            to_delete = verified_ids(keys, after)
            result.deleted = outbox_source.delete_ids(spark, s, to_delete)
            log.info("run_id=%s verified=%d deleted=%d conflicts_left_pending=%d",
                     run_id, len(to_delete), result.deleted, result.conflicts)
            batch.unpersist()

        audit_sink.record_run(spark, s, _run_row(result, started, "succeeded", None))
        return result
    except Exception as exc:
        log.error("run_id=%s failed: %s", run_id, exc)
        audit_sink.record_run(spark, s, _run_row(result, started, "failed", str(exc)))
        raise


def _run_row(r: RunResult, started: datetime, status: str, error: Optional[str]) -> dict:
    return {
        "run_id": r.run_id,
        "started_at": started,
        "finished_at": datetime.now(timezone.utc),
        "status": status,
        "read_count": r.read_count,
        "inserted": r.inserted,
        "duplicates": r.duplicates,
        "conflicts": r.conflicts,
        "deleted": r.deleted,
        "error": error,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = SparkSession.builder.appName("auditlake-ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        r = run(spark, Settings.from_env())
        print(f"RESULT {r}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
