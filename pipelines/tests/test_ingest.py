"""Pruebas contra el stack real (SQL Server + MinIO + Iceberg REST). Se ejecutan en serie."""

import dataclasses

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
