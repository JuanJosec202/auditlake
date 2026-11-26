"""Fase 5 contra el stack real: backfill idempotente + purga, compactación, retención y huérfanos."""

import dataclasses
import json
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from conftest import insert_event
from test_ingest import drain

from auditlake import maintenance as m
from auditlake import outbox_source
from auditlake.classify import legacy_event_id
from auditlake.main import backfill, purge_legacy, reconcile_legacy, run

CFG = m.MaintenanceConfig.load()
DEMO_TS = datetime(2025, 9, 1, 12, 0, 0)  # > 365 días: vencido según la retención local
EXPIRED_ID = legacy_event_id(1, DEMO_TS, source="demo", table="retention")
PROTECTED_ID = legacy_event_id(2, DEMO_TS, source="demo", table="retention")  # en conf/local/maintenance.json


def test_backfill_same_range_twice_then_purge_only_verified(spark, settings, outbox):
    day = "2024-03-10"
    outbox.execute(f"""
        DELETE FROM legacy.AuditLog WHERE OccurredAt >= '{day}' AND OccurredAt < DATEADD(day, 1, '{day}');
        INSERT INTO legacy.AuditLog (OccurredAt, EventType, TransferId, Amount, ApprovedBy)
        SELECT TOP (50)
               TODATETIMEOFFSET(DATEADD(minute, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)), '{day}'), '+00:00'),
               N'TransferApproved', NEWID(), 100.00, N'legacy.test'
        FROM sys.all_objects;""")
    first = backfill(spark, settings, day, "2024-03-11")
    second = backfill(spark, settings, day, "2024-03-11")
    print(f"\n[replay] corrida 1: {first}\n[replay] corrida 2: {second}")
    assert (first.read_count, first.inserted) == (50, 50)
    assert (second.read_count, second.inserted, second.duplicates) == (50, 0, 50)

    outbox.execute(f"UPDATE TOP (1) legacy.AuditLog SET Amount = 999.99 "
                   f"WHERE OccurredAt >= '{day}' AND OccurredAt < DATEADD(day, 1, '{day}')")
    report, verified = reconcile_legacy(spark, settings, day, "2024-03-11")
    purge = purge_legacy(spark, settings, day, "2024-03-11")
    print(f"[conciliación] {report}\n[purga] {purge}")
    assert report == {"sql_rows": 50, "archived_match": 49, "missing_in_bronze": 0, "hash_mismatch": 1}
    assert (purge["purged"], purge["left_in_sql"]) == (49, 1)


def test_compaction_keeps_event_hash_pairs(spark, settings):
    report = m.compact(spark, settings.events_table)
    print(f"\n[compactación] archivos {report['files_before']} -> {report['files_after']}, "
          f"máx. por partición {report['max_files_per_partition_before']} -> "
          f"{report['max_files_per_partition_after']}, pares {report.get('pairs')}")
    assert report["pairs"]["missing_after"] == 0 and report["pairs"]["extra_after"] == 0
    assert report["max_files_per_partition_after"] <= max(1, report["max_files_per_partition_before"])


def _in_current(spark, table, event_id):
    return spark.table(table).where(f"event_id = '{event_id}'").count()


def test_retention_expired_current_and_protected(spark, settings, outbox, sql_connection):
    drain(spark, settings, outbox)
    conn = sql_connection()
    insert_event(conn, DEMO_TS, event_id=EXPIRED_ID)
    insert_event(conn, DEMO_TS, event_id=PROTECTED_ID)
    _, current = insert_event(conn, datetime.now(timezone.utc).replace(tzinfo=None))
    run(spark, settings)
    t = settings.events_table
    assert all(_in_current(spark, t, e) == 1 for e in (EXPIRED_ID, PROTECTED_ID, current))

    r = m.retention(spark, t, CFG, expire_snapshots=False)
    print(f"\n[retención] {json.dumps(r, default=str)}")
    states = {name: _in_current(spark, t, e) for name, e in
              (("vencido", EXPIRED_ID), ("vigente", current), ("protegido", PROTECTED_ID))}
    tt_before = spark.sql(f"SELECT count(*) FROM {t} VERSION AS OF {r['snapshot_before_delete']} "
                          f"WHERE event_id = '{EXPIRED_ID}'").first()[0]
    print(f"[retención] snapshot actual: {states} | vencido con time travel al snapshot "
          f"{r['snapshot_before_delete']} (antes de expirar): {tt_before}")
    assert states == {"vencido": 0, "vigente": 1, "protegido": 1} and tt_before == 1

    ex = m.expire(spark, t, CFG)
    print(f"[expire_snapshots] expirados={ex['expired_snapshots']} quedan={ex['remaining_snapshots']}")
    assert r["snapshot_before_delete"] in ex["expired_snapshots"]
    with pytest.raises(Exception) as err:
        spark.sql(f"SELECT count(*) FROM {t} VERSION AS OF {r['snapshot_before_delete']}").collect()
    print(f"[retención] time travel después de expirar: {str(err.value).splitlines()[0][:140]}")


def test_technical_logs_shorter_retention(spark):
    t = "bronze.technical_logs"
    old, recent = datetime.now() - timedelta(days=45), datetime.now() - timedelta(days=10)
    tag = uuid.uuid4().hex[:8]
    old_msg, recent_msg = f"demo-retencion-45d-{tag}", f"demo-retencion-10d-{tag}"
    rows = [{"ts": ts, "level": "Information", "message": msg, "raw": msg, "run_id": "test",
             "ingested_at": datetime.now()}
            for ts, msg in ((old, old_msg), (recent, recent_msg))]
    spark.createDataFrame(rows, schema=spark.table(t).schema).writeTo(t).append()
    r = m.retention(spark, t, CFG)
    left = {msg: spark.table(t).where(f"message = '{msg}'").count()
            for msg in (old_msg, recent_msg)}
    print(f"\n[retención logs] días={r['retention_days']} corte={r['cutoff']} borradas={r['deleted_rows']} -> {left}")
    assert left == {old_msg: 0, recent_msg: 1}


def test_orphan_file_in_sandbox_dry_run_then_cleanup(spark):
    t = "sandbox.orphan_demo"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS sandbox")
    spark.sql(f"DROP TABLE IF EXISTS {t} PURGE")
    spark.sql(f"CREATE TABLE {t} (event_id string, payload_hash string) USING iceberg")
    spark.sql(f"INSERT INTO {t} VALUES ('e-1', 'h-1'), ('e-2', 'h-2')")
    jvm = spark.sparkContext._jvm
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, t)
    io = table.io()
    referenced = [r.file_path for r in spark.sql(f"SELECT file_path FROM {t}.files").collect()]
    orphan = table.location() + "/data/orphan-demo-00000.parquet"  # copia de un dato que ningún snapshot referencia
    src, dst = io.newInputFile(referenced[0]).newStream(), io.newOutputFile(orphan).createOrOverwrite()
    jvm.org.apache.commons.io.IOUtils.copy(src, dst)
    src.close()
    dst.close()

    safe = m.orphans(spark, t, CFG, dry_run=True)  # vía operativa: procedimiento + file_list_view, margen 3 días
    dry = m.orphans(spark, t, CFG, dry_run=True, sandbox_recent=True)
    print(f"\n[huérfanos] procedimiento (older_than {safe['older_than']}, dry_run): {safe['orphan_files']}")
    print(f"[huérfanos] dry_run sandbox: listados={dry['listed_files']} huérfanos={dry['orphan_files']} "
          f"existe_tras_dry_run={io.newInputFile(orphan).exists()}")
    assert safe["orphan_files"] == []  # el archivo es reciente: el margen operativo lo protege
    assert dry["orphan_files"] == [orphan] and io.newInputFile(orphan).exists()

    done = m.orphans(spark, t, CFG, dry_run=False, sandbox_recent=True)
    rows = spark.table(t).count()
    still = all(io.newInputFile(p).exists() for p in referenced)
    print(f"[huérfanos] limpieza: borrados={done['orphan_files']} existe={io.newInputFile(orphan).exists()} "
          f"referenciados_disponibles={still} filas={rows}")
    assert done["orphan_files"] == [orphan] and not io.newInputFile(orphan).exists()
    assert still and rows == 2
    spark.sql(f"DROP TABLE {t} PURGE")


def test_maintenance_respects_the_table_lock(spark, settings):
    holder = outbox_source.try_acquire_lock(spark, settings, outbox_source.LOCK_AUDIT_EVENTS)
    try:
        second = outbox_source.try_acquire_lock(spark, settings, outbox_source.LOCK_AUDIT_EVENTS)
        logs = outbox_source.try_acquire_lock(spark, settings, outbox_source.LOCK_TECHNICAL_LOGS)
        print(f"\n[locks] audit_events ocupado -> segundo intento: {second is not None}; "
              f"technical_logs libre: {logs is not None}")
        assert second is None and logs is not None
        logs.close()
    finally:
        holder.close()


def test_retention_pure_functions():
    now = datetime(2026, 11, 26)
    holds = [{"table": "bronze.audit_events", "event_id": "b", "until": None},
             {"table": "bronze.audit_events", "event_id": "a", "until": "2026-01-01T00:00:00"},
             {"table": "bronze.technical_logs", "event_id": "c", "until": None}]
    assert m.active_holds(holds, "bronze.audit_events", now) == ["b"]
    assert m.retention_cutoff(now, 30) == datetime(2026, 10, 27)
    assert m.retention_predicate("ts", datetime(2026, 10, 27), ["b"]) == (
        "ts < TIMESTAMP '2026-10-27 00:00:00' AND event_id NOT IN ('b')")
    assert dataclasses.is_dataclass(CFG) and CFG.retention_days["bronze.technical_logs"] < CFG.retention_days[
        "bronze.audit_events"]
