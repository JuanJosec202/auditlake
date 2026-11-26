"""Pruebas contra el stack real (SQL Server + MinIO + Iceberg REST). Se ejecutan en serie."""

import dataclasses
import time
from datetime import datetime, timedelta, timezone

import pytest
from conftest import insert_event

from auditlake import outbox_source
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


def _archived_row(spark, settings, event_id):
    return spark.table(settings.events_table).where(f"event_id = '{event_id}'").collect()


def _insert_conflict(outbox, event_id, occurred_at):
    """Fila entrante con un event_id ya archivado, otro payload y por lo tanto otro hash."""
    outbox.execute(f"""
        INSERT INTO audit.Outbox (EventId, EventType, SchemaVersion, OccurredAt, Payload, PayloadHash)
        VALUES ('{event_id}', 'TransferApproved', 1, '{occurred_at.isoformat()}+00:00', N'{{"tampered":true}}',
                REPLICATE('0', 64))
    """)


def test_conflict_goes_to_quarantine_and_batch_advances(spark, settings, approve_transfer, outbox):
    drain(spark, settings, outbox)
    archived = approve_transfer(amount=10)
    run(spark, settings)
    original = _archived_row(spark, settings, archived)[0]

    _insert_conflict(outbox, archived, original["occurred_at"])  # mismo evento y fecha, otro hash
    fresh = approve_transfer(amount=20)

    result = run(spark, settings)
    q = outbox.quarantine_row(archived)
    after = _archived_row(spark, settings, archived)
    print(f"\n[cuarentena] {result}")
    print(f"[cuarentena] fila: {q}")
    print(f"[cuarentena] original en Iceberg: filas={len(after)} hash={after[0]['payload_hash']} "
          f"occurred_at={after[0]['occurred_at']}")
    assert (result.read_count, result.inserted, result.conflicts, result.quarantined, result.deleted) == (2, 1, 1, 1, 1)
    assert outbox.pending(archived) == 0 and outbox.quarantined(archived) == 1
    assert q["payload_hash"] == "0" * 64 and q["archived_hash"] == original["payload_hash"]
    assert q["run_id"] == result.run_id and q["reason"]
    assert len(after) == 1 and after[0]["payload_hash"] == original["payload_hash"]
    assert after[0]["occurred_at"] == original["occurred_at"]
    assert outbox.pending(fresh) == 0 and len(_archived_row(spark, settings, fresh)) == 1
    assert run_row(spark, settings, result.run_id)["quarantined"] == 1


def test_archived_id_with_other_date_and_hash_is_quarantined(spark, settings, approve_transfer, outbox):
    drain(spark, settings, outbox)
    archived = approve_transfer(amount=30)
    run(spark, settings)
    original = _archived_row(spark, settings, archived)[0]

    other_date = original["occurred_at"] - timedelta(days=3)
    _insert_conflict(outbox, archived, other_date)
    result = run(spark, settings)

    rows = _archived_row(spark, settings, archived)
    q = outbox.quarantine_row(archived)
    print(f"\n[otra fecha] entrante occurred_at={other_date} | archivado occurred_at={original['occurred_at']}")
    print(f"[otra fecha] {result} | filas en Iceberg={len(rows)} pendientes={outbox.pending(archived)} "
          f"cuarentena occurred_at={q['occurred_at']}")
    assert (result.read_count, result.inserted, result.conflicts, result.quarantined, result.deleted) == (1, 0, 1, 1, 0)
    assert len(rows) == 1 and rows[0]["occurred_at"] == original["occurred_at"]
    assert outbox.pending(archived) == 0 and outbox.quarantined(archived) == 1
    assert q["occurred_at"].startswith(other_date.strftime("%Y-%m-%dT%H:%M:%S"))  # se conserva la fila entrante


def test_commit_order_higher_id_first(spark, settings, outbox, sql_connection):
    """101/102: la transacción de mayor Id confirma primero; la de menor Id, después."""
    drain(spark, settings, outbox)
    t1, t2 = sql_connection(), sql_connection()
    t1.setAutoCommit(False)
    t2.setAutoCommit(False)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    id1, ev1 = insert_event(t1, now)
    id2, ev2 = insert_event(t2, now + timedelta(milliseconds=5))
    assert id1 < id2
    t2.commit()  # 102 confirma; 101 sigue abierta

    # Evidencia de contención: la misma lectura SIN READPAST se bloquea en la fila de T1.
    probe = sql_connection()
    probe.createStatement().execute("SET LOCK_TIMEOUT 5000")
    started = time.monotonic()
    try:
        probe.createStatement().executeQuery("SELECT TOP (1000) Id FROM audit.Outbox ORDER BY Id").next()
        blocked = "no bloqueó"
    except Exception as exc:  # error 1222: Lock request time out period exceeded
        java = getattr(exc, "java_exception", None)
        msg = java.getMessage() if java is not None else str(exc).splitlines()[0]
        blocked = f"bloqueada {time.monotonic() - started:.1f} s -> {msg}"
    print(f"\n[101/102] Id T1={id1} (abierta) Id T2={id2} (confirmada)")
    print(f"[contención] lectura sin READPAST: {blocked}")

    first = run(spark, settings)
    print(f"[101/102] corrida con T1 abierta: {first}")
    assert (first.read_count, first.inserted, first.deleted) == (1, 1, 1)
    assert len(_archived_row(spark, settings, ev2)) == 1
    assert len(_archived_row(spark, settings, ev1)) == 0

    t1.commit()
    second = run(spark, settings)
    print(f"[101/102] corrida tras confirmar T1: {second}")
    assert (second.read_count, second.inserted, second.deleted) == (1, 1, 1)
    for ev in (ev1, ev2):
        assert len(_archived_row(spark, settings, ev)) == 1
        assert outbox.pending(ev) == 0
    print(f"[101/102] Iceberg: {ev1} x1, {ev2} x1; pendientes=0")


def test_second_ingestor_exits_before_reading(spark, settings, outbox, sql_connection, seed_events):
    drain(spark, settings, outbox)
    seed_events(datetime.now(timezone.utc).replace(tzinfo=None), 3)
    runs_before = spark.table(settings.runs_table).count()

    # simula un ingestor activo
    holder = outbox_source.try_acquire_lock(spark, settings, outbox_source.LOCK_AUDIT_EVENTS)
    assert holder is not None
    try:
        skipped = run(spark, settings)
    finally:
        holder.close()
    print(f"\n[ingestor único] con el lock ocupado: {skipped} | pendientes={outbox.total()}")
    assert skipped.status == "skipped" and skipped.read_count == 0
    assert outbox.total() == 3
    assert spark.table(settings.runs_table).count() == runs_before  # no escribió en Iceberg

    done = run(spark, settings)
    print(f"[ingestor único] lock libre: {done}")
    assert (done.status, done.deleted) == ("succeeded", 3)


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
    # 1 archivo nuevo por día; tras una compactación, el archivo compactado del mismo día puede solapar el rango.
    assert scanned <= 2 and scanned * 10 < total

    metrics = spark.sql(f"""
        SELECT partition.occurred_at_day AS day, record_count,
               readable_metrics.event_id.lower_bound AS event_id_min,
               readable_metrics.event_id.upper_bound AS event_id_max
        FROM {settings.events_table}.files ORDER BY day""").collect()
    print("[métricas de event_id por archivo]")
    for m in metrics:
        print(f"  {m['day']}  filas={m['record_count']:>3}  min={m['event_id_min']}  max={m['event_id_max']}")
    assert all(m["event_id_min"] and m["event_id_max"] for m in metrics)
