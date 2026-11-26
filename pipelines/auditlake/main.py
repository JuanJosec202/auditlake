"""Una corrida del ingestor: outbox (SQL Server) -> bronze.audit_events (Iceberg) -> DELETE verificado.

Ejecución manual y en serie, sin loop:
    docker compose run --rm spark python -m auditlake.main
    docker compose run --rm spark python -m auditlake.main --reconcile
    docker compose run --rm spark python -m auditlake.main --backfill --from 2026-05-01 --to 2026-11-01
    docker compose run --rm spark python -m auditlake.main --reconcile-legacy --from 2026-05-01 --to 2026-11-01
    docker compose run --rm spark python -m auditlake.main --purge-legacy --from 2026-05-01 --to 2026-11-01
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterator, List, Optional, Tuple

from pyspark.sql import SparkSession

from auditlake import audit_sink, outbox_source
from auditlake.classify import Outcome, classify, conflict_rows, count_outcomes, verified_ids
from auditlake.config import Settings

log = logging.getLogger("auditlake.ingest")


class InjectedFailure(RuntimeError):
    """Caída simulada entre el commit Iceberg y el DELETE en SQL (FAIL_AFTER_ICEBERG_COMMIT=1)."""


@dataclass
class RunResult:
    run_id: str
    status: str = "succeeded"
    read_count: int = 0
    inserted: int = 0
    duplicates: int = 0
    conflicts: int = 0
    quarantined: Optional[int] = 0
    deleted: int = 0
    mode: str = "outbox"


def run(spark: SparkSession, s: Settings, run_id: Optional[str] = None) -> RunResult:
    run_id = run_id or str(uuid.uuid4())
    result = RunResult(run_id)

    # Un solo ingestor activo: el segundo arranque sale aquí, antes de leer el lote o escribir en Iceberg.
    lock = outbox_source.try_acquire_lock(spark, s, outbox_source.LOCK_AUDIT_EVENTS)
    if lock is None:
        log.warning("run_id=%s otro ingestor activo (sp_getapplock '%s' ocupado): salgo sin procesar",
                    run_id, outbox_source.LOCK_AUDIT_EVENTS)
        result.status = "skipped"
        return result

    started = datetime.now(timezone.utc)
    try:
        log.info("run_id=%s acceso al catálogo Iceberg", run_id)
        audit_sink.ensure_tables(spark, s)
        _process(spark, s, result)
        audit_sink.record_run(spark, s, _run_row(result, started, None))
        return result
    except Exception as exc:
        result.status = "failed"
        log.error("run_id=%s failed: %s", run_id, exc)
        try:
            audit_sink.record_run(spark, s, _run_row(result, started, str(exc)))
        except Exception as record_exc:  # p. ej. MinIO caído: no ocultar el error original
            log.error("run_id=%s no se pudo registrar la corrida fallida: %s", run_id, record_exc)
        raise
    finally:
        lock.close()  # libera sp_getapplock


def _process(spark: SparkSession, s: Settings, result: RunResult) -> None:
    run_id = result.run_id
    batch = outbox_source.read_batch(spark, s)
    keys = [(r["outbox_id"], r["event_id"], r["payload_hash"]) for r in batch.collect()]
    result.read_count = len(keys)
    log.info("run_id=%s read=%d (ids fijados %s..%s)", run_id, result.read_count,
             keys[0][0] if keys else "-", keys[-1][0] if keys else "-")
    if not keys:
        return

    event_ids = [e for _, e, _ in keys]
    before = audit_sink.archived_hashes(spark, s, event_ids)
    counts = count_outcomes(((e, h) for _, e, h in keys), before)
    new_ids = [e for _, e, h in keys if classify(h, before.get(e)) is Outcome.NEW]
    result.inserted, result.duplicates, result.conflicts = counts.inserted, counts.duplicates, counts.conflicts

    if new_ids:
        audit_sink.merge_new(spark, s, batch, new_ids, run_id)
    log.info("run_id=%s merge: inserted=%d duplicates=%d conflicts=%d",
             run_id, result.inserted, result.duplicates, result.conflicts)

    if s.fail_after_iceberg_commit:
        raise InjectedFailure("FAIL_AFTER_ICEBERG_COMMIT=1: caída tras el commit Iceberg, antes del DELETE")

    # Verificación: releer Iceberg. Se borran solo los IDs fijados cuyo event_id y hash coinciden;
    # los conflictos pasan a audit.OutboxQuarantine en la misma transacción.
    after = audit_sink.archived_hashes(spark, s, event_ids)
    to_delete = verified_ids(keys, after)
    conflicts = conflict_rows(keys, after)
    result.deleted, result.quarantined = outbox_source.settle(spark, s, to_delete, conflicts, run_id)
    log.info("run_id=%s verified=%d deleted=%d quarantined=%d",
             run_id, len(to_delete), result.deleted, result.quarantined)
    batch.unpersist()


def _run_row(r: RunResult, started: datetime, error: Optional[str]) -> dict:
    return {
        "run_id": r.run_id,
        "started_at": started,
        "finished_at": datetime.now(timezone.utc),
        "status": r.status,
        "read_count": r.read_count,
        "inserted": r.inserted,
        "duplicates": r.duplicates,
        "conflicts": r.conflicts,
        "deleted": r.deleted,
        "error": error,
        "quarantined": r.quarantined,
        "mode": r.mode,
    }


def reconcile(spark: SparkSession, s: Settings) -> dict:
    """Conciliación por event_id: bronze sin duplicados, pendientes en SQL y conflictos en cuarentena."""
    bronze = spark.table(s.events_table)
    report = {
        "bronze_rows": bronze.count(),
        "bronze_distinct_event_ids": bronze.select("event_id").distinct().count(),
        "outbox_pending": outbox_source.query_scalar(spark, s, "SELECT COUNT_BIG(*) FROM audit.Outbox"),
        "quarantine_rows": outbox_source.query_scalar(spark, s, "SELECT COUNT_BIG(*) FROM audit.OutboxQuarantine"),
        "quarantine_not_in_bronze": 0,
    }
    report["bronze_duplicate_event_ids"] = report["bronze_rows"] - report["bronze_distinct_event_ids"]
    quarantined = outbox_source.quarantined_event_ids(spark, s)
    if quarantined:
        found = bronze.where(bronze.event_id.isin(quarantined)).select("event_id").distinct().count()
        report["quarantine_not_in_bronze"] = len(set(quarantined)) - found
    return report


# --- Histórico (una vez): legacy.AuditLog -> bronze.audit_events -> conciliación -> purga ---------------

def _chunks(start: str, end: str, days: int) -> Iterator[Tuple[str, str]]:
    d, stop = date.fromisoformat(start), date.fromisoformat(end)
    while d < stop:
        nxt = min(d + timedelta(days=days), stop)
        yield d.isoformat(), nxt.isoformat()
        d = nxt


def backfill(spark: SparkSession, s: Settings, start: str, end: str, chunk_days: int = 31,
             run_id: Optional[str] = None) -> RunResult:
    """Carga [start, end) de legacy.AuditLog en bronze con event_id determinista. Idempotente: reejecutar
    el mismo rango solo cuenta duplicados. Comparte el lock de audit_events con el ingestor."""
    run_id = run_id or str(uuid.uuid4())
    result = RunResult(run_id, quarantined=None, mode="backfill")
    lock = outbox_source.try_acquire_lock(spark, s, outbox_source.LOCK_AUDIT_EVENTS)
    if lock is None:
        log.warning("run_id=%s otro proceso escribe bronze.audit_events: salgo sin procesar", run_id)
        result.status = "skipped"
        return result
    started = datetime.now(timezone.utc)
    try:
        audit_sink.ensure_tables(spark, s)
        for c_start, c_end in _chunks(start, end, chunk_days):
            legacy = outbox_source.read_legacy(spark, s, c_start, c_end)
            events = audit_sink.legacy_to_events(legacy).cache()
            keys = [(r["legacy_id"], r["event_id"], r["payload_hash"]) for r in events.collect()]
            if keys:
                event_ids = [e for _, e, _ in keys]
                before = audit_sink.archived_hashes(spark, s, event_ids)
                counts = count_outcomes(((e, h) for _, e, h in keys), before)
                new_ids = [e for _, e, h in keys if classify(h, before.get(e)) is Outcome.NEW]
                if new_ids:
                    audit_sink.merge_new(spark, s, events, new_ids, run_id)
                result.read_count += len(keys)
                result.inserted += counts.inserted
                result.duplicates += counts.duplicates
                result.conflicts += counts.conflicts
                log.info("run_id=%s backfill [%s, %s): read=%d inserted=%d duplicates=%d conflicts=%d",
                         run_id, c_start, c_end, len(keys), counts.inserted, counts.duplicates, counts.conflicts)
            events.unpersist()
            legacy.unpersist()
        audit_sink.record_run(spark, s, _run_row(result, started, None))
        return result
    except Exception as exc:
        result.status = "failed"
        log.error("run_id=%s backfill failed: %s", run_id, exc)
        audit_sink.record_run(spark, s, _run_row(result, started, str(exc)))
        raise
    finally:
        lock.close()


def reconcile_legacy(spark: SparkSession, s: Settings, start: str, end: str,
                     chunk_days: int = 31) -> Tuple[dict, List[int]]:
    """Concilia [start, end) por (event_id, payload_hash) recalculados desde SQL contra bronze.
    Devuelve el reporte y los Id de legacy que hoy coinciden exactamente con lo archivado."""
    report = {"sql_rows": 0, "archived_match": 0, "missing_in_bronze": 0, "hash_mismatch": 0}
    verified: List[int] = []
    for c_start, c_end in _chunks(start, end, chunk_days):
        legacy = outbox_source.read_legacy(spark, s, c_start, c_end)
        keys = [(r["legacy_id"], r["event_id"], r["payload_hash"])
                for r in audit_sink.legacy_to_events(legacy).collect()]
        legacy.unpersist()
        if not keys:
            continue
        archived = audit_sink.archived_hashes(spark, s, [e for _, e, _ in keys])
        report["sql_rows"] += len(keys)
        for legacy_id, event_id, payload_hash in keys:
            if event_id not in archived:
                report["missing_in_bronze"] += 1
            elif archived[event_id] != payload_hash:
                report["hash_mismatch"] += 1
            else:
                report["archived_match"] += 1
                verified.append(legacy_id)
    return report, verified


def purge_legacy(spark: SparkSession, s: Settings, start: str, end: str) -> dict:
    """Vuelve a conciliar justo antes de borrar y purga solo las filas que todavía coinciden con bronze."""
    lock = outbox_source.try_acquire_lock(spark, s, outbox_source.LOCK_AUDIT_EVENTS)
    if lock is None:
        return {"status": "skipped"}
    try:
        report, verified = reconcile_legacy(spark, s, start, end)
        report["purged"] = outbox_source.purge_legacy_ids(spark, s, verified)
        report["left_in_sql"] = report["sql_rows"] - report["purged"]
        return report
    finally:
        lock.close()


def main(argv: Optional[list] = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    spark = SparkSession.builder.appName("auditlake-ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        s = Settings.from_env()
        p = argparse.ArgumentParser(prog="auditlake.main")
        mode = p.add_mutually_exclusive_group()
        mode.add_argument("--reconcile", action="store_true")
        mode.add_argument("--backfill", action="store_true")
        mode.add_argument("--reconcile-legacy", action="store_true")
        mode.add_argument("--purge-legacy", action="store_true")
        p.add_argument("--from", dest="start")
        p.add_argument("--to", dest="end")
        a = p.parse_args(argv)
        if (a.backfill or a.reconcile_legacy or a.purge_legacy) and not (a.start and a.end):
            p.error("--from y --to son obligatorios para el histórico")
        if a.reconcile:
            print("RECONCILE", reconcile(spark, s))
            print("QUARANTINE", outbox_source.quarantine_report(spark, s))
        elif a.backfill:
            print(f"BACKFILL {backfill(spark, s, a.start, a.end)}")
        elif a.reconcile_legacy:
            print(f"RECONCILE_LEGACY {reconcile_legacy(spark, s, a.start, a.end)[0]}")
        elif a.purge_legacy:
            print(f"PURGE_LEGACY {purge_legacy(spark, s, a.start, a.end)}")
        else:
            print(f"RESULT {run(spark, s)}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
