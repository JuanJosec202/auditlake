"""Bordes de I/O con Iceberg: tablas bronze, MERGE idempotente y relectura para verificar."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from auditlake.config import Settings


def ensure_tables(spark: SparkSession, s: Settings) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {s.events_table} (
            event_id       string    NOT NULL,
            event_type     string    NOT NULL,
            schema_version int       NOT NULL,
            occurred_at    timestamp NOT NULL,
            payload        string    NOT NULL,
            payload_hash   string    NOT NULL,
            run_id         string,
            ingested_at    timestamp
        ) USING iceberg
        PARTITIONED BY (days(occurred_at))
        TBLPROPERTIES ('format-version' = '2')
        """
    )
    spark.sql(f"ALTER TABLE {s.events_table} WRITE ORDERED BY event_type, occurred_at")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {s.runs_table} (
            run_id      string NOT NULL,
            started_at  timestamp,
            finished_at timestamp,
            status      string,
            read_count  bigint,
            inserted    bigint,
            duplicates  bigint,
            conflicts   bigint,
            deleted     bigint,
            error       string
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def occurred_range(batch: DataFrame) -> Tuple[datetime, datetime]:
    row = batch.agg(F.min("occurred_at").alias("lo"), F.max("occurred_at").alias("hi")).first()
    return row["lo"], row["hi"]


def _range_predicate(column: str, lo: datetime, hi: datetime) -> str:
    return f"{column} BETWEEN TIMESTAMP '{lo.isoformat(sep=' ')}' AND TIMESTAMP '{hi.isoformat(sep=' ')}'"


def archived_hashes(spark: SparkSession, s: Settings, batch: DataFrame) -> Dict[str, str]:
    """event_id -> payload_hash en bronze para los eventos del lote, con poda por occurred_at."""
    lo, hi = occurred_range(batch)
    existing = spark.table(s.events_table).where(_range_predicate("occurred_at", lo, hi))
    rows = existing.join(batch.select("event_id"), "event_id").select("event_id", "payload_hash").collect()
    return {r["event_id"]: r["payload_hash"] for r in rows}


def merge_new(spark: SparkSession, s: Settings, batch: DataFrame, run_id: str) -> None:
    """Solo WHEN NOT MATCHED THEN INSERT: un evento ya archivado nunca se toca."""
    lo, hi = occurred_range(batch)
    (
        batch.select("event_id", "event_type", "schema_version", "occurred_at", "payload", "payload_hash")
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .createOrReplaceTempView("outbox_batch")
    )
    spark.sql(
        f"""
        MERGE INTO {s.events_table} t
        USING outbox_batch b
        ON t.event_id = b.event_id AND {_range_predicate("t.occurred_at", lo, hi)}
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def record_run(spark: SparkSession, s: Settings, run: dict) -> None:
    spark.createDataFrame([run], schema=spark.table(s.runs_table).schema).writeTo(s.runs_table).append()
