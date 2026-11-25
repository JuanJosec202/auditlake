"""Pruebas contra el stack real (SQL Server + MinIO + Iceberg REST). Se ejecutan en serie."""

import dataclasses
from datetime import datetime, timedelta, timezone

import pytest

from auditlake.main import InjectedFailure, run


def iceberg_rows(spark, settings, event_id):
    return spark.table(settings.events_table).where(f"event_id = '{event_id}'").count()


def run_row(spark, settings, run_id):
    return spark.table(settings.runs_table).where(f"run_id = '{run_id}'").first().asDict()


def drain(spark, settings, outbox):
    """Deja la outbox sin pendientes para que cada prueba controle exactamente su lote."""
    while outbox.total() > 0:
        before = outbox.total()
        run(spark, settings)
        assert outbox.total() < before, "la outbox no drena (¿conflictos pendientes de otra prueba?)"


def test_crash_after_iceberg_commit_then_retry(spark, settings, approve_transfer, outbox):
    drain(spark, settings, outbox)
    event_id = approve_transfer(amount=4200)

    crashing = dataclasses.replace(settings, fail_after_iceberg_commit=True)
    with pytest.raises(InjectedFailure):
        run(spark, crashing, run_id="crash-" + event_id)

    print(f"\n[tras la caída] event_id={event_id} filas en Iceberg={iceberg_rows(spark, settings, event_id)} "
          f"pendientes en SQL={outbox.pending(event_id)}")
    assert iceberg_rows(spark, settings, event_id) == 1
    assert outbox.pending(event_id) == 1

    retry = run(spark, settings, run_id="retry-" + event_id)

    rows, pending, runrow = iceberg_rows(spark, settings, event_id), outbox.pending(event_id), run_row(
        spark, settings, retry.run_id)
    print(f"[tras el reintento] event_id={event_id} filas en Iceberg={rows} pendientes en SQL={pending}")
    print(f"[ingestion_runs] {runrow}")
    assert rows == 1
    assert pending == 0
    assert (runrow["read_count"], runrow["inserted"], runrow["duplicates"], runrow["deleted"]) == (1, 0, 1, 1)


def test_conflict_stays_pending_and_run_continues(spark, settings, approve_transfer, outbox):
    drain(spark, settings, outbox)
    archived = approve_transfer(amount=10)
    run(spark, settings)

    # Mismo evento (event_id y occurred_at) que uno ya archivado, pero con otro payload y por lo tanto otro hash.
    occurred = spark.table(settings.events_table).where(f"event_id = '{archived}'").first()["occurred_at"]
    outbox.execute(f"""
        INSERT INTO audit.Outbox (EventId, EventType, SchemaVersion, OccurredAt, Payload, PayloadHash)
        VALUES ('{archived}', 'TransferApproved', 1, '{occurred.isoformat()}+00:00', N'{{"tampered":true}}',
                REPLICATE('0', 64))
    """)
    fresh = approve_transfer(amount=20)

    result = run(spark, settings)
    print(f"\n[conflicto] {result}")
    try:
        assert (result.read_count, result.inserted, result.conflicts, result.deleted) == (2, 1, 1, 1)
        assert outbox.pending(archived) == 1  # la fila en conflicto queda en SQL
        assert outbox.pending(fresh) == 0  # el resto del lote sí se drenó
        assert iceberg_rows(spark, settings, archived) == 1  # bronze no se sobrescribe
    finally:
        outbox.execute(f"DELETE FROM audit.Outbox WHERE EventId = '{archived}'")


def test_archived_id_with_other_date_and_hash_is_conflict(spark, settings, approve_transfer, outbox):
    drain(spark, settings, outbox)
    archived = approve_transfer(amount=30)
    run(spark, settings)
    original = spark.table(settings.events_table).where(f"event_id = '{archived}'").first()["occurred_at"]

    other_date = original - timedelta(days=3)
    outbox.execute(f"""
        INSERT INTO audit.Outbox (EventId, EventType, SchemaVersion, OccurredAt, Payload, PayloadHash)
        VALUES ('{archived}', 'TransferApproved', 1, '{other_date.isoformat()}+00:00', N'{{"tampered":true}}',
                REPLICATE('0', 64))
    """)
    try:
        result = run(spark, settings)
        rows = spark.table(settings.events_table).where(f"event_id = '{archived}'").collect()
        print(f"\n[otra fecha] entrante occurred_at={other_date} | archivado occurred_at={original}")
        print(f"[otra fecha] {result} | filas en Iceberg={len(rows)} pendientes en SQL={outbox.pending(archived)}")
        assert (result.read_count, result.inserted, result.conflicts, result.deleted) == (1, 0, 1, 0)
        assert len(rows) == 1 and rows[0]["occurred_at"] == original
        assert outbox.pending(archived) == 1
    finally:
        outbox.execute(f"DELETE FROM audit.Outbox WHERE EventId = '{archived}'")


def _planned_files(spark, settings, event_ids=None):
    """Archivos que el planificador de Iceberg incluye para el mismo rango de event_id que usa el ingestor."""
    jvm = spark.sparkContext._jvm
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, settings.events_table)
    scan = table.newScan()
    if event_ids:
        e = jvm.org.apache.iceberg.expressions.Expressions
        scan = scan.filter(getattr(e, "and")(e.greaterThanOrEqual("event_id", min(event_ids)),
                                              e.lessThanOrEqual("event_id", max(event_ids))))
    it, n = scan.planFiles().iterator(), 0
    while it.hasNext():
        it.next()
        n += 1
    return n


def test_event_id_lookup_skips_files_of_other_days(spark, settings, outbox, seed_events):
    drain(spark, settings, outbox)
    base = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=10)
    by_day = {}
    for d in range(5):  # 5 días, 20 eventos por día, una corrida por día
        by_day[d] = seed_events(base + timedelta(days=d), 20)
        assert run(spark, settings).inserted == 20

    lookup = by_day[2]
    total, scanned = _planned_files(spark, settings), _planned_files(spark, settings, lookup)
    print(f"\n[poda] búsqueda de 20 event_id del día {base.date() + timedelta(days=2)}: "
          f"archivos escaneados={scanned} de {total} archivos totales")
    assert scanned == 1 < total

    metrics = spark.sql(f"""
        SELECT partition.occurred_at_day AS day, record_count,
               readable_metrics.event_id.lower_bound AS event_id_min,
               readable_metrics.event_id.upper_bound AS event_id_max
        FROM {settings.events_table}.files ORDER BY day""").collect()
    print("[métricas de event_id por archivo]")
    for m in metrics:
        print(f"  {m['day']}  filas={m['record_count']:>3}  min={m['event_id_min']}  max={m['event_id_max']}")
    assert all(m["event_id_min"] and m["event_id_max"] for m in metrics)
