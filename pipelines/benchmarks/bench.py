"""Herramientas de medición de la fase 6 (no forman parte del pipeline). Cada subcomando corre en su propio
contenedor del servicio spark, con los mismos límites de recursos (docker-compose.bench.yml).

    approve        aprobaciones vía API con concurrencia fija: p50, p95 y errores por corrida
    query-sql      bucle de la consulta histórica contra legacy.AuditLog (JDBC)
    query-iceberg  bucle de la misma consulta en Spark sobre silver.audit_events
    ingest-loop    ingestor de la outbox en bucle (escenario "ingestor activo")
    waits          esperas por bloqueo (delta de LCK_M_%) y sesiones bloqueadas muestreadas

Los bucles terminan cuando aparece /app/benchmarks/results/STOP.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import List, Optional

RESULTS = "/app/benchmarks/results"
STOP = os.path.join(RESULTS, "STOP")
API = os.environ.get("API_URL", "http://api:8080")
RANGE = ("2026-05-01", "2026-11-01")

SQL_QUERY = f"""
SELECT CONVERT(char(10), CAST(SWITCHOFFSET(OccurredAt, '+00:00') AS date), 23) AS d,
       COUNT_BIG(*) AS n, SUM(Amount) AS total
FROM legacy.AuditLog
WHERE OccurredAt >= '{RANGE[0]} 00:00:00 +00:00' AND OccurredAt < '{RANGE[1]} 00:00:00 +00:00'
GROUP BY CAST(SWITCHOFFSET(OccurredAt, '+00:00') AS date)
ORDER BY d"""

ICEBERG_QUERY = f"""
SELECT date_format(cast(occurred_at AS date), 'yyyy-MM-dd') AS d, count(*) AS n, sum(amount) AS total
FROM silver.audit_events
WHERE occurred_at >= TIMESTAMP '{RANGE[0]} 00:00:00' AND occurred_at < TIMESTAMP '{RANGE[1]} 00:00:00'
GROUP BY cast(occurred_at AS date)
ORDER BY d"""


def percentile(values: List[float], p: float) -> float:
    """Percentil por interpolación lineal (función pura)."""
    if not values:
        return float("nan")
    xs = sorted(values)
    k = (len(xs) - 1) * p
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def summarize(latencies_ms: List[float], errors: int) -> dict:
    return {
        "requests": len(latencies_ms) + errors,
        "errors": errors,
        "p50_ms": round(percentile(latencies_ms, 0.50), 1),
        "p95_ms": round(percentile(latencies_ms, 0.95), 1),
        "max_ms": round(max(latencies_ms), 1) if latencies_ms else None,
    }


def _save(name: str, data: dict) -> None:
    os.makedirs(RESULTS, exist_ok=True)
    with open(os.path.join(RESULTS, name + ".json"), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str, ensure_ascii=False)
    print(f"BENCH {name} {json.dumps(data, default=str, ensure_ascii=False)}")


def _post(path: str, body: dict) -> dict:
    req = urllib.request.Request(API + path, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def approve(n: int, concurrency: int, runs: int, label: str) -> None:
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        ids = list(pool.map(lambda i: _post("/transfers", {"amount": 100 + i % 900})["id"], range(n * runs)))
    out = {"label": label, "n_per_run": n, "concurrency": concurrency, "runs": []}

    def one(tid: str):
        t0 = time.perf_counter()
        try:
            _post(f"/transfers/{tid}/approve", {"approvedBy": "bench"})
            return (time.perf_counter() - t0) * 1000, None
        except Exception as exc:  # noqa: BLE001 - se cuentan como errores
            return None, str(exc)[:120]

    # Calentamiento sin medir (JIT, pool de conexiones, planes en caché): 500 aprobaciones.
    warm = [_post("/transfers", {"amount": 1})["id"] for _ in range(500)]
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(one, warm))
    for r in range(runs):
        batch = ids[r * n:(r + 1) * n]
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(one, batch))
        lat = [x for x, e in results if x is not None]
        errs = [e for x, e in results if e is not None]
        s = summarize(lat, len(errs))
        s.update(run=r + 1, seconds=round(time.time() - t0, 1), sample_errors=errs[:3])
        out["runs"].append(s)
    p95s = [x["p95_ms"] for x in out["runs"]]
    out["median_p95_ms"] = statistics.median(p95s)
    _save(f"approve-{label}", out)


def _spark():
    from pyspark.sql import SparkSession

    s = SparkSession.builder.appName("auditlake-bench").getOrCreate()
    s.sparkContext.setLogLevel("ERROR")
    return s


def _settings():
    sys.path.insert(0, "/app")
    from auditlake.config import Settings

    return Settings.from_env()


def _rows_to_totals(rows) -> dict:
    return {r[0]: [int(r[1]), str(Decimal(str(r[2])).quantize(Decimal("0.01")))] for r in rows}


def query_loop(engine: str, label: str, max_seconds: int) -> None:
    t_start = time.time()
    spark = _spark()
    startup_s = time.time() - t_start
    lat, totals = [], None
    if engine == "sql":
        s = _settings()
        from auditlake import outbox_source

        def run_once():
            conn = outbox_source._connection(spark, s)
            try:
                rs = conn.createStatement().executeQuery(SQL_QUERY)
                rows = []
                while rs.next():
                    rows.append((rs.getString(1), rs.getLong(2), str(rs.getBigDecimal(3))))
                return rows
            finally:
                conn.close()
    else:
        def run_once():
            return [(r.d, r.n, r.total) for r in spark.sql(ICEBERG_QUERY).collect()]

    first_with_startup = None
    while time.time() - t_start < max_seconds and not os.path.exists(STOP):
        t0 = time.time()
        rows = run_once()
        elapsed = (time.time() - t0) * 1000
        if first_with_startup is None:
            first_with_startup = round(startup_s * 1000 + elapsed, 1)
            totals = _rows_to_totals(rows)
        lat.append(elapsed)
    _save(f"query-{engine}-{label}", {
        "engine": engine, "executions": len(lat),
        "spark_session_startup_ms": round(startup_s * 1000, 1),
        "first_query_including_startup_ms": first_with_startup,
        "p50_ms": round(percentile(lat, 0.5), 1), "p95_ms": round(percentile(lat, 0.95), 1),
        "days": len(totals or {}), "rows_total": sum(v[0] for v in (totals or {}).values()),
        "amount_total": str(sum(Decimal(v[1]) for v in (totals or {}).values())),
        "totals_by_day": totals,
    })
    spark.stop()


def ingest_loop(label: str, max_seconds: int) -> None:
    spark = _spark()
    s = _settings()
    from auditlake.main import run

    t_start, runs, archived, failed, errors = time.time(), 0, 0, 0, []
    while time.time() - t_start < max_seconds and not os.path.exists(STOP):
        try:
            r = run(spark, s)
        except Exception as exc:  # noqa: BLE001 - como en operación: la corrida falla y la siguiente reintenta
            failed += 1
            errors.append(str(exc).splitlines()[0][:160])
            time.sleep(2)
            continue
        runs += 1
        archived += r.deleted
        if r.read_count == 0:
            time.sleep(2)
    _save(f"ingest-{label}", {"runs_ok": runs, "runs_failed": failed, "archived": archived,
                              "read_hint": s.read_hint, "sample_errors": sorted(set(errors))[:3]})
    spark.stop()


def waits(label: str, max_seconds: int) -> None:
    spark = _spark()
    s = _settings()
    from auditlake import outbox_source

    conn = outbox_source._connection(spark, s)
    st = conn.createStatement()
    st.execute("DBCC SQLPERF('sys.dm_os_wait_stats', CLEAR) WITH NO_INFOMSGS")
    rcsi = outbox_source.query_scalar(spark, s, "SELECT CAST(is_read_committed_snapshot_on AS bigint) "
                                             "FROM sys.databases WHERE name = DB_NAME()")
    t_start, samples, blocked_samples, max_blocked = time.time(), 0, 0, 0
    while time.time() - t_start < max_seconds and not os.path.exists(STOP):
        rs = st.executeQuery("SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id <> 0")
        rs.next()
        b = rs.getInt(1)
        samples += 1
        blocked_samples += 1 if b else 0
        max_blocked = max(max_blocked, b)
        time.sleep(0.25)
    rs = st.executeQuery("SELECT wait_type, waiting_tasks_count, wait_time_ms FROM sys.dm_os_wait_stats "
                         "WHERE wait_type LIKE 'LCK_M_%' AND waiting_tasks_count > 0 ORDER BY wait_time_ms DESC")
    lck = []
    while rs.next():
        lck.append({"wait_type": rs.getString(1), "tasks": rs.getLong(2), "wait_ms": rs.getLong(3)})
    conn.close()
    _save(f"waits-{label}", {
        "rcsi": bool(rcsi), "samples": samples, "samples_with_blocking": blocked_samples,
        "max_blocked_sessions": max_blocked, "lock_wait_tasks": sum(x["tasks"] for x in lck),
        "lock_wait_ms": sum(x["wait_ms"] for x in lck), "by_type": lck,
    })
    spark.stop()


def main(argv: Optional[list] = None) -> None:
    p = argparse.ArgumentParser(prog="bench")
    p.add_argument("command", choices=["approve", "query-sql", "query-iceberg", "ingest-loop", "waits"])
    p.add_argument("--label", required=True)
    p.add_argument("--n", type=int, default=5000)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--max-seconds", type=int, default=900)
    a = p.parse_args(argv)
    if a.command == "approve":
        approve(a.n, a.concurrency, a.runs, a.label)
    elif a.command in ("query-sql", "query-iceberg"):
        query_loop(a.command.split("-")[1], a.label, a.max_seconds)
    elif a.command == "ingest-loop":
        ingest_loop(a.label, a.max_seconds)
    else:
        waits(a.label, a.max_seconds)


if __name__ == "__main__":
    main()
