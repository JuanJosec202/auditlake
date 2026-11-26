"""Evento v2 y payload malformado por el camino real: outbox -> ingestor -> bronze -> dbt (silver/gold)."""

import json
from datetime import datetime, timezone

from conftest import insert_event
from dbt.cli.main import dbtRunner
from test_ingest import drain

from auditlake.main import run


def _dbt_build():
    res = dbtRunner().invoke(["build", "--project-dir", "/dbt", "--profiles-dir", "/dbt", "--quiet"])
    return res, {r.node.name: str(r.status) for r in res.result}


def test_v2_event_is_normalized_and_malformed_payload_warns(spark, settings, outbox, sql_connection):
    drain(spark, settings, outbox)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn = sql_connection()
    _, v1 = insert_event(conn, now)
    _, v2 = insert_event(conn, now, payload=json.dumps(
        {"transfer": {"id": "t-v2-001", "amount": {"value": 1250.5, "currency": "COP"}},
         "approver": {"userId": "ana.supervisora"}, "approvedAt": now.isoformat() + "Z"},
        separators=(",", ":")), schema_version=2)
    _, bad = insert_event(conn, now, payload='{"transferId": "t-bad", "amount": ', schema_version=1)  # JSON truncado
    assert run(spark, settings).inserted == 3

    try:
        res, status = _dbt_build()
        print(f"\n[dbt build] success={res.success} " + " ".join(f"{k}={v}" for k, v in sorted(status.items())))
        rows = spark.sql(f"""
            SELECT event_id, schema_version, transfer_id, amount, currency, approved_by
            FROM silver.audit_events WHERE event_id IN ('{v1}', '{v2}', '{bad}') ORDER BY schema_version DESC
        """).collect()
        for r in rows:
            print(f"[silver] {r.asDict()}")
        failures = spark.sql(
            "SELECT * FROM dbt_test__audit.not_null_audit_events_amount").where(f"event_id = '{bad}'").count()
        print("[store_failures] dbt_test__audit.not_null_audit_events_amount contiene el malformado:",
              failures == 1)
        gold = spark.sql(
            f"SELECT * FROM gold.audit_daily WHERE day = DATE '{now.date()}' ORDER BY currency").collect()
        for g in gold:
            print(f"[gold] {g.asDict()}")

        assert res.success  # warn no rompe el build
        assert status["not_null_audit_events_amount"] == "warn"
        assert status["not_null_audit_events_transfer_id"] == "warn"
        assert status["unique_audit_events_event_id"] == "pass"
        assert status["reconcile_ingestion_runs"] == "pass"
        by_id = {r.event_id: r for r in rows}
        assert (by_id[v2].transfer_id, float(by_id[v2].amount), by_id[v2].currency, by_id[v2].approved_by) == (
            "t-v2-001", 1250.5, "COP", "ana.supervisora")
        assert by_id[v1].currency is None and by_id[v1].amount is not None
        assert by_id[bad].amount is None and failures == 1
        assert any(g.currency is None and g.total_amount is None and g.events > 0 for g in gold)
        assert any(g.currency == "COP" and g.total_amount is not None for g in gold)
    finally:
        # Limpieza de la prueba: el malformado no queda permanente en bronze (DELETE de Iceberg, como la retención).
        spark.sql(f"DELETE FROM {settings.events_table} WHERE event_id = '{bad}'")
