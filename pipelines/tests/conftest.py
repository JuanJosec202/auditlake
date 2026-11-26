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

        def quarantined(self, event_id):
            return outbox_source.query_scalar(
                spark, settings, f"SELECT COUNT_BIG(*) FROM audit.OutboxQuarantine WHERE EventId = '{event_id}'")

        def quarantine_row(self, event_id):
            rows = outbox_source.quarantine_report(spark, settings, limit=1000)
            return next(r for r in rows if r["event_id"] == event_id)
    return _Outbox()


def uuid7_at(ts):
    """UUIDv7 con el timestamp dado (48 bits de ms + versión + aleatorio), igual que Guid.CreateVersion7."""
    import uuid as _uuid
    ms = int(ts.timestamp() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    value = (ms << 80) | (0x7 << 76) | (((rand >> 62) & 0xFFF) << 64) | (0b10 << 62) | (rand & ((1 << 62) - 1))
    return str(_uuid.UUID(int=value))


def insert_event(conn, ts, event_id=None, payload=None):
    """INSERT de un evento en audit.Outbox con la conexión dada (respeta su transacción). Devuelve (Id, event_id)."""
    import hashlib

    event_id = event_id or uuid7_at(ts)
    payload = payload or json.dumps({"transferId": event_id, "amount": 10, "approvedBy": "seed"},
                                    separators=(",", ":"))
    ps = conn.prepareStatement(
        "SET NOCOUNT ON; "
        "INSERT INTO audit.Outbox (EventId, EventType, SchemaVersion, OccurredAt, Payload, PayloadHash) "
        "VALUES (?, 'TransferApproved', 1, CAST(? AS datetimeoffset), ?, ?); SELECT CAST(SCOPE_IDENTITY() AS bigint);")
    ps.setString(1, event_id)
    ps.setString(2, ts.strftime("%Y-%m-%d %H:%M:%S.%f") + " +00:00")
    ps.setString(3, payload)
    ps.setString(4, hashlib.sha256(payload.encode()).hexdigest())
    rs = ps.executeQuery()
    rs.next()
    return rs.getLong(1), event_id


@pytest.fixture
def sql_connection(spark, settings):
    """Abre conexiones JDBC "de aplicación" (sin los ajustes del worker) y las cierra al final."""
    opened = []

    def _open():
        conn = outbox_source._connection(spark, settings)
        opened.append(conn)
        return conn
    yield _open
    for conn in opened:
        try:
            conn.close()
        except Exception:
            pass


@pytest.fixture
def seed_events(spark, settings):
    """Inserta n eventos TransferApproved en la outbox con fecha del día dado; devuelve sus event_id."""
    import hashlib
    from datetime import timedelta

    def _seed(day, n):
        conn = outbox_source._connection(spark, settings)
        try:
            ps = conn.prepareStatement(
                "INSERT INTO audit.Outbox (EventId, EventType, SchemaVersion, OccurredAt, Payload, PayloadHash) "
                "VALUES (?, 'TransferApproved', 1, CAST(? AS datetimeoffset), ?, ?)")
            ids = []
            for i in range(n):
                ts = day + timedelta(seconds=i)
                event_id = uuid7_at(ts)
                payload = json.dumps({"transferId": event_id, "amount": 10 + i, "approvedBy": "seed"},
                                     separators=(",", ":"))
                ps.setString(1, event_id)
                ps.setString(2, ts.strftime("%Y-%m-%d %H:%M:%S.%f") + " +00:00")
                ps.setString(3, payload)
                ps.setString(4, hashlib.sha256(payload.encode()).hexdigest())
                ps.addBatch()
                ids.append(event_id)
            ps.executeBatch()
            return ids
        finally:
            conn.close()
    return _seed
