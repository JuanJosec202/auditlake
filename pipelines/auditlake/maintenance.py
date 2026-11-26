"""Mantenimiento de tablas Iceberg con procedimientos de Iceberg (nunca sintaxis de Delta).

Cada operación sobre una tabla toma el mismo sp_getapplock que su escritor (único escritor por tabla):
    bronze.audit_events   -> auditlake:audit_events   (ingestor, backfill, mantenimiento)
    bronze.technical_logs -> auditlake:technical_logs (log_ingest, retención)
Las tablas del namespace sandbox son de ensayo y no usan lock.

    docker compose run --rm spark python -m auditlake.maintenance compact   --table bronze.audit_events
    docker compose run --rm spark python -m auditlake.maintenance manifests --table bronze.audit_events
    docker compose run --rm spark python -m auditlake.maintenance retention --table bronze.audit_events
    docker compose run --rm spark python -m auditlake.maintenance orphans   --table bronze.audit_events --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

from auditlake import outbox_source
from auditlake.config import Settings

log = logging.getLogger("auditlake.maintenance")

CONFIG_PATH = "/opt/spark/conf/maintenance.json"
LOCKS = {
    "bronze.audit_events": outbox_source.LOCK_AUDIT_EVENTS,
    "bronze.technical_logs": outbox_source.LOCK_TECHNICAL_LOGS,
}
TIME_COLUMN = {"bronze.audit_events": "occurred_at", "bronze.technical_logs": "ts"}


@dataclass(frozen=True)
class MaintenanceConfig:
    retention_days: Dict[str, int]
    legal_holds: List[dict] = field(default_factory=list)
    snapshot_max_age_hours: float = 168
    snapshot_retain_last: int = 1
    rewrite_manifests_min_manifests: int = 50
    orphan_older_than_days: int = 3

    @staticmethod
    def load(path: str = CONFIG_PATH) -> MaintenanceConfig:
        with open(path, encoding="utf-8") as f:
            return MaintenanceConfig(**json.load(f))


# --- Funciones puras --------------------------------------------------------------------------------

def retention_cutoff(now: datetime, days: int) -> datetime:
    return now - timedelta(days=days)


def active_holds(holds: Sequence[dict], table: str, now: datetime) -> List[str]:
    """event_id con bloqueo legal vigente para la tabla (until nulo = indefinido)."""
    out = []
    for h in holds:
        until = h.get("until")
        if h.get("table") == table and (until is None or datetime.fromisoformat(until) > now):
            out.append(h["event_id"])
    return sorted(set(out))


def retention_predicate(time_column: str, cutoff: datetime, held_ids: Sequence[str]) -> str:
    """Filas vencidas que no tienen bloqueo legal. Los ids salen de la configuración, no del usuario final."""
    pred = f"{time_column} < TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'"
    if held_ids:
        pred += " AND event_id NOT IN (" + ", ".join(f"'{i}'" for i in held_ids) + ")"
    return pred


# --- Bordes con Spark / Iceberg ---------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def files_per_partition(spark: SparkSession, table: str) -> List[dict]:
    rows = spark.sql(f"""
        SELECT CAST(partition AS string) AS partition, count(*) AS files, sum(file_size_in_bytes) AS bytes
        FROM {table}.files GROUP BY partition ORDER BY partition""").collect()
    return [r.asDict() for r in rows]


def current_snapshot(spark: SparkSession, table: str) -> Optional[int]:
    row = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
    return row[0] if row else None


def compare_pairs(spark: SparkSession, table: str, before_snapshot: int) -> dict:
    """Compara los pares (event_id, payload_hash) del snapshot anterior contra el actual, en ambos sentidos."""
    before = spark.sql(f"SELECT event_id, payload_hash FROM {table} VERSION AS OF {before_snapshot}")
    after = spark.sql(f"SELECT event_id, payload_hash FROM {table}")
    return {
        "pairs_before": before.count(),
        "pairs_after": after.count(),
        "missing_after": before.exceptAll(after).count(),
        "extra_after": after.exceptAll(before).count(),
    }


def compact(spark: SparkSession, table: str) -> dict:
    before_files = files_per_partition(spark, table)
    before_snapshot = current_snapshot(spark, table)
    res = spark.sql(f"""
        CALL lake.system.rewrite_data_files(
            table => '{table}',
            strategy => 'sort',
            sort_order => 'event_type ASC NULLS LAST, occurred_at ASC NULLS LAST',
            options => map('min-input-files', '2'))""").first().asDict()
    after_files = files_per_partition(spark, table)
    report = {
        "rewrite": res,
        "files_before": sum(p["files"] for p in before_files),
        "files_after": sum(p["files"] for p in after_files),
        "partitions": len(after_files),
        "max_files_per_partition_before": max((p["files"] for p in before_files), default=0),
        "max_files_per_partition_after": max((p["files"] for p in after_files), default=0),
        "by_partition_before": {p["partition"]: p["files"] for p in before_files if p["files"] > 1},
        "by_partition_after": {p["partition"]: p["files"] for p in after_files if p["partition"]
                               in {q["partition"] for q in before_files if q["files"] > 1}},
    }
    if table == "bronze.audit_events" and before_snapshot is not None:
        report["pairs"] = compare_pairs(spark, table, before_snapshot)
    return report


def manifests(spark: SparkSession, table: str, cfg: MaintenanceConfig) -> dict:
    count = spark.sql(f"SELECT count(*) FROM {table}.manifests").first()[0]
    report = {"manifests": count, "threshold": cfg.rewrite_manifests_min_manifests}
    if count > cfg.rewrite_manifests_min_manifests:
        report["rewrite"] = spark.sql(f"CALL lake.system.rewrite_manifests(table => '{table}')").first().asDict()
    else:
        report["rewrite"] = "omitido: no hay acumulación que lo justifique"
    return report


def expire(spark: SparkSession, table: str, cfg: MaintenanceConfig) -> dict:
    before = [r[0] for r in spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").collect()]
    older_than = _now() - timedelta(hours=cfg.snapshot_max_age_hours)
    res = spark.sql(f"""
        CALL lake.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S.%f')}',
            retain_last => {int(cfg.snapshot_retain_last)})""").first().asDict()
    after = [r[0] for r in spark.sql(f"SELECT snapshot_id FROM {table}.snapshots").collect()]
    return {"expired_snapshots": sorted(set(before) - set(after)), "remaining_snapshots": after, "result": res}


def retention(spark: SparkSession, table: str, cfg: MaintenanceConfig, now: Optional[datetime] = None,
              expire_snapshots: bool = True) -> dict:
    now = now or _now()
    column = TIME_COLUMN[table]
    cutoff = retention_cutoff(now, cfg.retention_days[table])
    held = active_holds(cfg.legal_holds, table, now) if table == "bronze.audit_events" else []
    t = spark.table(table)
    expired_total = t.where(f"{column} < TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'").count()
    held_expired = t.where(retention_predicate(column, cutoff, [])).where(t.event_id.isin(held)).count() if held else 0
    snapshot_before_delete = current_snapshot(spark, table)
    predicate = retention_predicate(column, cutoff, held)
    spark.sql(f"DELETE FROM {table} WHERE {predicate}")
    report = {
        "table": table,
        "retention_days": cfg.retention_days[table],
        "cutoff": cutoff.isoformat(sep=" "),
        "expired_rows": expired_total,
        "held_rows_kept": held_expired,
        "deleted_rows": expired_total - held_expired,
        "snapshot_before_delete": snapshot_before_delete,
        "snapshot_after_delete": current_snapshot(spark, table),
    }
    # Sin expirar, el DELETE solo publica un snapshot nuevo: lo borrado sigue legible con time travel.
    if expire_snapshots:
        report["expire"] = expire(spark, table, cfg)
    return report


def list_table_files(spark: SparkSession, table: str) -> DataFrame:
    """Listado real de archivos bajo la ubicación de la tabla con el FileIO de Iceberg (S3FileIO). Evita el
    FileSystem de Hadoop, que en esta imagen (Hadoop 3.3.4 sin hadoop-aws) no conoce el esquema s3://."""
    jvm = spark.sparkContext._jvm
    t = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    rows, it = [], t.io().listPrefix(t.location()).iterator()
    while it.hasNext():
        fi = it.next()
        rows.append((fi.location(), datetime.fromtimestamp(fi.createdAtMillis() / 1000, timezone.utc)
                     .replace(tzinfo=None)))
    return spark.createDataFrame(rows, "file_path string, last_modified timestamp")


def orphans(spark: SparkSession, table: str, cfg: MaintenanceConfig, dry_run: bool,
            sandbox_recent: bool = False) -> dict:
    listing = list_table_files(spark, table)
    view = "orphan_listing_" + table.replace(".", "_")
    listing.createOrReplaceTempView(view)
    report = {"listed_files": listing.count()}
    if not sandbox_recent:
        older_than = _now() - timedelta(days=cfg.orphan_older_than_days)
        rows = spark.sql(f"""
            CALL lake.system.remove_orphan_files(
                table => '{table}',
                older_than => TIMESTAMP '{older_than.strftime('%Y-%m-%d %H:%M:%S')}',
                dry_run => {str(dry_run).lower()},
                file_list_view => '{view}')""").collect()
        report.update(mode="procedure", older_than=older_than.isoformat(sep=" "),
                      orphan_files=[r[0] for r in rows])
        return report

    # Ensayo en sandbox con un archivo recién creado: el procedimiento exige older_than de al menos 24 h,
    # así que se usa la Action API (la vía que la propia validación indica) con olderThan = ahora. Solo se
    # permite en el namespace sandbox, donde no hay escritores concurrentes.
    if not table.startswith("sandbox."):
        raise ValueError("--sandbox-recent solo se permite en tablas del namespace sandbox")
    from pyspark.java_gateway import ensure_callback_server_started

    jvm = spark.sparkContext._jvm
    t = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    action = (jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
              .deleteOrphanFiles(t)
              .olderThan(int(datetime.now(timezone.utc).timestamp() * 1000))
              .compareToFileList(listing._jdf))
    if dry_run:  # dry run: la función de borrado solo registra la ruta
        ensure_callback_server_started(spark.sparkContext._gateway)

        class _Collect:
            def __init__(self) -> None:
                self.paths: List[str] = []

            def accept(self, path: str) -> None:
                self.paths.append(path)

            class Java:
                implements = ["java.util.function.Consumer"]

        action = action.deleteWith(_Collect())
    result = action.execute()
    report.update(mode="action_api_sandbox", dry_run=dry_run,
                  orphan_files=[p for p in result.orphanFileLocations()])
    return report


def main(argv: Optional[list] = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(prog="auditlake.maintenance")
    p.add_argument("command", choices=["compact", "manifests", "expire", "retention", "orphans"])
    p.add_argument("--table", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--sandbox-recent", action="store_true")
    a = p.parse_args(sys.argv[1:] if argv is None else argv)

    cfg = MaintenanceConfig.load()
    spark = SparkSession.builder.appName("auditlake-maintenance").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    lock = None
    try:
        resource = LOCKS.get(a.table)
        if resource is None and not a.table.startswith("sandbox."):
            raise SystemExit(f"tabla sin lock configurado: {a.table}")
        if resource:
            lock = outbox_source.try_acquire_lock(spark, Settings.from_env(), resource)
            if lock is None:
                log.warning("otro proceso escribe %s (sp_getapplock '%s'): salgo sin mantenimiento", a.table, resource)
                print(f"MAINTENANCE skipped table={a.table} lock={resource}")
                return 0
        if a.command == "compact":
            out = compact(spark, a.table)
        elif a.command == "manifests":
            out = manifests(spark, a.table, cfg)
        elif a.command == "expire":
            out = expire(spark, a.table, cfg)
        elif a.command == "retention":
            out = retention(spark, a.table, cfg)
        else:
            out = orphans(spark, a.table, cfg, a.dry_run, a.sandbox_recent)
        print(f"MAINTENANCE {a.command} {json.dumps(out, default=str, ensure_ascii=False)}")
        return 0
    finally:
        if lock is not None:
            lock.close()
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
