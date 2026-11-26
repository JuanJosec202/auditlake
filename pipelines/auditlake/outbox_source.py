"""Bordes de I/O con SQL Server: lectura del lote pendiente y borrado de filas verificadas."""

from __future__ import annotations

from datetime import datetime
from typing import List, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession

from auditlake.config import Settings

# Ajustes solo para las sesiones SQL del worker (lectura de Spark y conexiones de DELETE); la API no los usa.
WORKER_SESSION_SETTINGS = "SET LOCK_TIMEOUT 5000; SET DEADLOCK_PRIORITY LOW;"

# Lee filas existentes en orden de PK. Nunca un watermark por ID máximo: una transacción
# más lenta puede confirmar un Id menor después (caso 101/102). READPAST salta las filas de
# transacciones aún abiertas (sin bloquearse ni depender de RCSI); se toman en la pasada siguiente.
_BATCH_QUERY = """
SELECT TOP ({n})
    Id                                                        AS outbox_id,
    LOWER(CONVERT(char(36), EventId))                         AS event_id,
    EventType                                                 AS event_type,
    SchemaVersion                                             AS schema_version,
    CONVERT(datetime2(6), SWITCHOFFSET(OccurredAt, '+00:00')) AS occurred_at,
    Payload                                                   AS payload,
    PayloadHash                                               AS payload_hash
FROM audit.Outbox WITH (READPAST)
ORDER BY Id
"""


def read_batch(spark: SparkSession, s: Settings) -> DataFrame:
    """Devuelve el lote materializado (cache + count) para que JDBC no se re-evalúe."""
    df = (
        spark.read.format("jdbc")
        .option("url", s.jdbc_url)
        .option("user", s.user)
        .option("password", s.password)
        .option("query", _BATCH_QUERY.format(n=int(s.batch_size)))
        .option("sessionInitStatement", WORKER_SESSION_SETTINGS)
        .load()
        .cache()
    )
    df.count()
    return df


def _connection(spark: SparkSession, s: Settings, worker: bool = False):
    """Conexión JDBC con el mismo driver que usa Spark (mssql-jdbc), sin dependencias Python extra."""
    jvm = spark.sparkContext._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", s.user)
    props.setProperty("password", s.password)
    conn = jvm.com.microsoft.sqlserver.jdbc.SQLServerDriver().connect(s.jdbc_url, props)
    if worker:
        conn.createStatement().execute(WORKER_SESSION_SETTINGS)
    return conn


# Un lock por tabla Iceberg (único escritor). audit_events: ingestor, backfill y su mantenimiento.
# technical_logs: log_ingest y su retención.
LOCK_AUDIT_EVENTS = "auditlake:audit_events"
LOCK_TECHNICAL_LOGS = "auditlake:technical_logs"


def try_acquire_lock(spark: SparkSession, s: Settings, resource: str):
    """sp_getapplock exclusivo con dueño Session y espera 0. Devuelve la conexión que lo sostiene
    (cerrarla lo libera; si el proceso muere, SQL Server lo libera al cerrar la sesión) o None si
    otro proceso ya escribe esa tabla."""
    conn = _connection(spark, s, worker=True)
    rs = conn.createStatement().executeQuery(
        "SET NOCOUNT ON; DECLARE @r int; "
        f"EXEC @r = sp_getapplock @Resource = N'{resource}', @LockMode = 'Exclusive', "
        "@LockOwner = 'Session', @LockTimeout = 0; SELECT @r;"
    )
    rs.next()
    if rs.getInt(1) >= 0:
        return conn
    conn.close()
    return None


QUARANTINE_REASON = "event_id ya archivado en bronze con otro payload_hash"

# Mueve la fila entrante completa (columnas explícitas) a la cuarentena y la retira de la outbox en una
# sola sentencia. El filtro por PayloadHash evita mover una fila distinta a la clasificada.
_QUARANTINE_SQL = """
DELETE FROM audit.Outbox
OUTPUT DELETED.Id, DELETED.EventId, DELETED.EventType, DELETED.SchemaVersion, DELETED.OccurredAt,
       DELETED.Payload, DELETED.PayloadHash, ?, ?, ?
INTO audit.OutboxQuarantine
       (OutboxId, EventId, EventType, SchemaVersion, OccurredAt,
        Payload, PayloadHash, ArchivedHash, Reason, RunId)
WHERE Id = ? AND PayloadHash = ?
"""


def settle(
    spark: SparkSession,
    s: Settings,
    verified: Sequence[int],
    conflicts: Sequence[Tuple[int, str, str]],
    run_id: str,
) -> Tuple[int, int]:
    """Borra las filas verificadas por PK en lotes y mueve los conflictos a audit.OutboxQuarantine.
    Los conflictos y el primer lote de borrado van en la misma transacción.
    conflicts: (outbox_id, payload_hash entrante, payload_hash archivado). Devuelve (deleted, quarantined)."""
    if not verified and not conflicts:
        return 0, 0
    conn = _connection(spark, s, worker=True)
    try:
        conn.setAutoCommit(False)
        quarantined = 0
        if conflicts:
            ps = conn.prepareStatement(_QUARANTINE_SQL)
            for outbox_id, payload_hash, archived_hash in conflicts:
                ps.setString(1, archived_hash)
                ps.setString(2, QUARANTINE_REASON)
                ps.setString(3, run_id)
                ps.setLong(4, int(outbox_id))
                ps.setString(5, payload_hash)
                quarantined += ps.executeUpdate()
        stmt = conn.createStatement()
        deleted = 0
        chunks = [verified[i : i + s.delete_chunk] for i in range(0, len(verified), s.delete_chunk)] or [[]]
        for chunk in chunks:
            if chunk:
                ids = ",".join(str(int(x)) for x in chunk)
                deleted += stmt.executeUpdate(f"DELETE FROM audit.Outbox WHERE Id IN ({ids})")
            conn.commit()
        return deleted, quarantined
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query_scalar(spark: SparkSession, s: Settings, sql: str) -> int:
    conn = _connection(spark, s)
    try:
        rs = conn.createStatement().executeQuery(sql)
        rs.next()
        return int(rs.getLong(1))
    finally:
        conn.close()


def execute(spark: SparkSession, s: Settings, sql: str) -> None:
    conn = _connection(spark, s)
    try:
        conn.createStatement().execute(sql)
    finally:
        conn.close()


def pending_ids(spark: SparkSession, s: Settings, event_id: str) -> List[int]:
    conn = _connection(spark, s)
    try:
        ps = conn.prepareStatement("SELECT Id FROM audit.Outbox WHERE EventId = ?")
        ps.setString(1, event_id)
        rs = ps.executeQuery()
        out = []
        while rs.next():
            out.append(int(rs.getLong(1)))
        return out
    finally:
        conn.close()


def quarantined_event_ids(spark: SparkSession, s: Settings) -> List[str]:
    conn = _connection(spark, s)
    try:
        rs = conn.createStatement().executeQuery(
            "SELECT DISTINCT LOWER(CONVERT(char(36), EventId)) FROM audit.OutboxQuarantine")
        out = []
        while rs.next():
            out.append(rs.getString(1))
        return out
    finally:
        conn.close()


def quarantine_report(spark: SparkSession, s: Settings, limit: int = 20) -> List[dict]:
    conn = _connection(spark, s)
    try:
        rs = conn.createStatement().executeQuery(
            f"SELECT TOP ({int(limit)}) OutboxId, LOWER(CONVERT(char(36), EventId)), "
            "CONVERT(varchar(33), OccurredAt, 126), PayloadHash, ArchivedHash, Reason, RunId, "
            "CONVERT(varchar(33), QuarantinedAt, 126) FROM audit.OutboxQuarantine ORDER BY Id DESC")
        cols = ["outbox_id", "event_id", "occurred_at", "payload_hash", "archived_hash", "reason", "run_id",
                "quarantined_at"]
        out = []
        while rs.next():
            out.append({c: rs.getString(i + 1) for i, c in enumerate(cols)})
        return out
    finally:
        conn.close()


# --- legacy.AuditLog (histórico, una vez) ------------------------------------------------------------

_LEGACY_QUERY = """
SELECT
    Id                                                        AS legacy_id,
    CONVERT(datetime2(6), SWITCHOFFSET(OccurredAt, '+00:00')) AS occurred_at,
    EventType                                                 AS event_type,
    LOWER(CONVERT(char(36), TransferId))                      AS transfer_id,
    Amount                                                    AS amount,
    ApprovedBy                                                AS approved_by
FROM legacy.AuditLog
WHERE OccurredAt >= '{start}' AND OccurredAt < '{end}'
"""


def read_legacy(spark: SparkSession, s: Settings, start: str, end: str) -> DataFrame:
    """Filas históricas en [start, end) (fechas UTC 'YYYY-MM-DD'), materializadas (cache + count)."""
    for d in (start, end):  # se interpolan en el SQL: validar el formato
        datetime.strptime(d, "%Y-%m-%d")
    df = (
        spark.read.format("jdbc")
        .option("url", s.jdbc_url)
        .option("user", s.user)
        .option("password", s.password)
        .option("query", _LEGACY_QUERY.format(start=start + " 00:00:00 +00:00", end=end + " 00:00:00 +00:00"))
        .option("sessionInitStatement", WORKER_SESSION_SETTINGS)
        .load()
        .cache()
    )
    df.count()
    return df


def purge_legacy_ids(spark: SparkSession, s: Settings, ids: Sequence[int], chunk: int = 1000,
                     pause: str = "00:00:00.100") -> int:
    """Borra de legacy.AuditLog solo los Id verificados, en lotes pequeños (cada uno su transacción) con una
    pausa entre lotes para no competir con la carga del core."""
    if not ids:
        return 0
    conn = _connection(spark, s, worker=True)
    try:
        conn.setAutoCommit(True)
        stmt = conn.createStatement()
        deleted = 0
        for i in range(0, len(ids), chunk):
            batch = ",".join(str(int(x)) for x in ids[i : i + chunk])
            deleted += stmt.executeUpdate(f"DELETE FROM legacy.AuditLog WHERE Id IN ({batch})")
            stmt.execute(f"WAITFOR DELAY '{pause}'")
        return deleted
    finally:
        conn.close()
