"""Bordes de I/O con SQL Server: lectura del lote pendiente y borrado de filas verificadas."""

from __future__ import annotations

from typing import List, Sequence

from pyspark.sql import DataFrame, SparkSession

from auditlake.config import Settings

# Lee filas existentes en orden de PK. Nunca un watermark por ID máximo: una transacción
# más lenta puede confirmar un Id menor después (caso 101/102).
_BATCH_QUERY = """
SELECT TOP ({n})
    Id                                                        AS outbox_id,
    LOWER(CONVERT(char(36), EventId))                         AS event_id,
    EventType                                                 AS event_type,
    SchemaVersion                                             AS schema_version,
    CONVERT(datetime2(6), SWITCHOFFSET(OccurredAt, '+00:00')) AS occurred_at,
    Payload                                                   AS payload,
    PayloadHash                                               AS payload_hash
FROM audit.Outbox
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
        .load()
        .cache()
    )
    df.count()
    return df


def _connection(spark: SparkSession, s: Settings):
    """Conexión JDBC con el mismo driver que usa Spark (mssql-jdbc), sin dependencias Python extra."""
    jvm = spark.sparkContext._jvm
    props = jvm.java.util.Properties()
    props.setProperty("user", s.user)
    props.setProperty("password", s.password)
    return jvm.com.microsoft.sqlserver.jdbc.SQLServerDriver().connect(s.jdbc_url, props)


def delete_ids(spark: SparkSession, s: Settings, ids: Sequence[int]) -> int:
    """Borra por PK en lotes; cada lote es su propia transacción corta."""
    if not ids:
        return 0
    conn = _connection(spark, s)
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        deleted = 0
        for i in range(0, len(ids), s.delete_chunk):
            chunk = ",".join(str(int(x)) for x in ids[i : i + s.delete_chunk])
            deleted += stmt.executeUpdate(f"DELETE FROM audit.Outbox WHERE Id IN ({chunk})")
            conn.commit()
        return deleted
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
