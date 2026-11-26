"""Demo de punta a punta con los módulos existentes (no agrega lógica de negocio).

    cp .env.example .env
    docker compose -p auditlake-demo run --build --rm spark python -m auditlake.demo

Pasos: esperar la API -> sembrar legacy.AuditLog -> tráfico de aprobaciones -> ingestor (outbox) ->
backfill x2 -> conciliación y purga del histórico -> log_ingest -> dbt build -> compactación, manifests y
retención -> conciliación final. Imprime un resumen con cifras y tiempos.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Tuple

from pyspark.sql import SparkSession

from auditlake import log_ingest, maintenance, outbox_source
from auditlake.config import Settings
from auditlake.main import backfill, purge_legacy, reconcile, run

log = logging.getLogger("auditlake.demo")
API = os.environ.get("API_URL", "http://api:8080")
LEGACY_FROM, LEGACY_TO = "2026-05-01", "2026-11-01"
TRANSFERS = int(os.environ.get("DEMO_TRANSFERS", "200"))


def _post(path: str, body: dict) -> dict:
    req = urllib.request.Request(API + path, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def wait_for_api(timeout_s: int = 180) -> int:
    deadline = time.time() + timeout_s
    while True:
        try:
            with urllib.request.urlopen(API + "/health", timeout=5) as resp:
                if resp.status == 200:
                    return 200
        except Exception:
            if time.time() > deadline:
                raise
        time.sleep(2)


def seed_legacy(spark: SparkSession, s: Settings) -> int:
    with open("/sql/seed_legacy_auditlog.sql", encoding="utf-8") as f:
        script = f.read()
    outbox_source.execute(spark, s, script.split("SELECT COUNT_BIG(*)")[0])
    return outbox_source.query_scalar(spark, s, "SELECT COUNT_BIG(*) FROM legacy.AuditLog")


def traffic(n: int) -> int:
    def one(i: int) -> int:
        t = _post("/transfers", {"amount": 100 + i})
        _post(f"/transfers/{t['id']}/approve", {"approvedBy": f"demo.user{i % 5}"})
        return 1

    with ThreadPoolExecutor(max_workers=8) as pool:
        return sum(pool.map(one, range(n)))


def drain_outbox(spark: SparkSession, s: Settings) -> Dict[str, int]:
    totals = {"runs": 0, "read": 0, "inserted": 0, "deleted": 0, "quarantined": 0}
    while outbox_source.query_scalar(spark, s, "SELECT COUNT_BIG(*) FROM audit.Outbox") > 0:
        r = run(spark, s)
        totals["runs"] += 1
        totals["read"] += r.read_count
        totals["inserted"] += r.inserted
        totals["deleted"] += r.deleted
        totals["quarantined"] += r.quarantined or 0
        if r.read_count == 0:
            break
    return totals


def ingest_logs(spark: SparkSession, s: Settings, wait_s: int = 150) -> Dict[str, object]:
    """La rotación de Serilog es por minuto y el sellado deja 1 minuto de margen: espera a que haya
    archivos cerrados que sellar (máx. wait_s) y corre una ingesta availableNow."""
    paths = log_ingest.Paths()
    lock = outbox_source.try_acquire_lock(spark, s, outbox_source.LOCK_TECHNICAL_LOGS)
    if lock is None:
        return {"status": "skipped"}
    try:
        deadline, moved = time.time() + wait_s, []
        while not moved and time.time() < deadline:
            moved = log_ingest.seal(paths)
            if not moved:
                time.sleep(10)
        rows = log_ingest.ingest(spark, paths, "demo")
        return {"sealed": len(moved), "numInputRows": rows, "technical_logs": spark.table(paths.table).count()}
    finally:
        lock.close()


def dbt_build() -> Dict[str, object]:
    from dbt.cli.main import dbtRunner

    res = dbtRunner().invoke(["build", "--project-dir", "/dbt", "--profiles-dir", "/dbt", "--quiet"])
    statuses = [str(r.status) for r in res.result]
    return {"success": res.success, "nodes": len(statuses),
            **{k: statuses.count(k) for k in ("success", "pass", "warn", "error", "fail")}}


def with_lock(spark: SparkSession, s: Settings, resource: str, fn: Callable[[], dict]) -> dict:
    lock = outbox_source.try_acquire_lock(spark, s, resource)
    if lock is None:
        return {"status": "skipped"}
    try:
        return fn()
    finally:
        lock.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    started = time.time()
    steps: List[Tuple[str, float, object]] = []

    def step(name: str, fn: Callable[[], object]) -> object:
        t0 = time.time()
        out = fn()
        steps.append((name, time.time() - t0, out))
        log.info("DEMO %-28s %6.1f s  %s", name, time.time() - t0, out)
        return out

    step("API disponible", wait_for_api)
    spark = SparkSession.builder.appName("auditlake-demo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    s = Settings.from_env()
    cfg = maintenance.MaintenanceConfig.load()
    try:
        step("siembra legacy.AuditLog", lambda: {"filas": seed_legacy(spark, s)})
        step("aprobaciones vía API", lambda: {"transferencias_aprobadas": traffic(TRANSFERS)})
        step("ingestor (outbox)", lambda: drain_outbox(spark, s))
        step("backfill corrida 1", lambda: backfill(spark, s, LEGACY_FROM, LEGACY_TO).__dict__)
        step("backfill corrida 2", lambda: backfill(spark, s, LEGACY_FROM, LEGACY_TO).__dict__)
        step("conciliación + purga", lambda: purge_legacy(spark, s, LEGACY_FROM, LEGACY_TO))
        step("log_ingest", lambda: ingest_logs(spark, s))
        step("dbt build", dbt_build)
        t = s.events_table
        step("compactación", lambda: with_lock(spark, s, outbox_source.LOCK_AUDIT_EVENTS, lambda: {
            k: v for k, v in maintenance.compact(spark, t).items() if not k.startswith("by_partition")}))
        step("manifests", lambda: with_lock(spark, s, outbox_source.LOCK_AUDIT_EVENTS,
                                            lambda: maintenance.manifests(spark, t, cfg)))
        step("retención audit_events", lambda: with_lock(spark, s, outbox_source.LOCK_AUDIT_EVENTS, lambda: {
            k: v for k, v in maintenance.retention(spark, t, cfg).items() if k != "expire"}))
        step("conciliación final", lambda: reconcile(spark, s))
    finally:
        spark.stop()

    print("\n================ RESUMEN DEMO AUDITLAKE ================")
    for name, secs, out in steps:
        print(f"{name:<28} {secs:7.1f} s  {json.dumps(out, default=str, ensure_ascii=False)}")
    print(f"{'TOTAL (dentro del contenedor)':<28} {time.time() - started:7.1f} s")


if __name__ == "__main__":
    main()
