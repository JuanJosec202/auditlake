import json
import os
import urllib.request

import pytest
from pyspark.sql import SparkSession

from auditlake import outbox_source
from auditlake.config import Settings


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.appName("auditlake-tests").getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def settings():
    return Settings.from_env()


def _post(path, body):
    req = urllib.request.Request(
        os.environ["API_URL"] + path, data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


@pytest.fixture
def approve_transfer(spark, settings):
    """Crea y aprueba una transferencia por la API; devuelve el event_id de su fila en la outbox."""
    def _approve(amount=100):
        transfer = _post("/transfers", {"amount": amount})
        _post(f"/transfers/{transfer['id']}/approve", {"approvedBy": "pytest"})
        conn = outbox_source._connection(spark, settings)
        try:
            ps = conn.prepareStatement(
                "SELECT LOWER(CONVERT(char(36), EventId)) FROM audit.Outbox WHERE Payload LIKE ?")
            ps.setString(1, f"%{transfer['id']}%")
            rs = ps.executeQuery()
            assert rs.next(), "la aprobación no dejó fila en audit.Outbox"
            return rs.getString(1)
        finally:
            conn.close()
    return _approve


@pytest.fixture
def outbox(spark, settings):
    class _Outbox:
        def pending(self, event_id):
            return len(outbox_source.pending_ids(spark, settings, event_id))

        def total(self):
            return outbox_source.query_scalar(spark, settings, "SELECT COUNT_BIG(*) FROM audit.Outbox")

        def execute(self, sql):
            outbox_source.execute(spark, settings, sql)
    return _Outbox()
