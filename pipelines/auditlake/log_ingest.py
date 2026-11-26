"""Hiper-logs (best effort): archivos CLEF de Serilog -> bronze.technical_logs. Nunca pasan por SQL Server.

1. Sellado: mueve de active/ a ready/ solo archivos cerrados tras la rotación, con un margen de 1 minuto.
   El archivo activo (el más reciente) nunca se mueve: Spark no debe leer un archivo que aún crece.
2. readStream sobre ready/ con trigger availableNow y checkpoint persistente: cada archivo se carga una vez.
3. cleanSource=archive mueve los archivos ya procesados a archive/ (fuera de ready/).

Ejecución manual:  docker compose run --rm spark python -m auditlake.log_ingest
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

log = logging.getLogger("auditlake.log_ingest")

# api-202611260038.clef o api-202611260038_001.clef (rotación por tamaño dentro del mismo minuto)
_FILE_RE = re.compile(r"^api-(\d{12})(?:_\d+)?\.clef$")
ROTATION = timedelta(minutes=1)  # debe coincidir con rollingInterval de Serilog (demo)
MARGIN = timedelta(minutes=1)

_CLEF = StructType(
    [
        StructField("@t", StringType()),
        StructField("@m", StringType()),
        StructField("@mt", StringType()),
        StructField("@l", StringType()),
        StructField("@x", StringType()),
        StructField("@tr", StringType()),
        StructField("SourceContext", StringType()),
        StructField("RequestPath", StringType()),
        StructField("RequestMethod", StringType()),
        StructField("StatusCode", IntegerType()),
        StructField("Elapsed", DoubleType()),
    ]
)


@dataclass(frozen=True)
class Paths:
    active: str = "/data/logs/active"
    ready: str = "/data/logs/ready"
    archive: str = "/data/logs/archive"
    checkpoint: str = "/data/checkpoints/technical_logs"
    table: str = "bronze.technical_logs"


def sealable(names: Iterable[str], now: datetime, rotation: timedelta = ROTATION,
             margin: timedelta = MARGIN) -> List[str]:
    """Archivos que se pueden mover a ready/: su periodo de rotación terminó hace al menos `margin`,
    y nunca el archivo activo (el periodo más reciente). Función pura."""
    parsed = []
    for name in names:
        m = _FILE_RE.match(name)
        if m:
            start = datetime.strptime(m.group(1), "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            parsed.append((name, start))
    if not parsed:
        return []
    newest = max(start for _, start in parsed)
    return sorted(
        name for name, start in parsed
        if start < newest and start + rotation + margin <= now
    )


def seal(paths: Paths, now: Optional[datetime] = None) -> List[str]:
    now = now or datetime.now(timezone.utc)
    os.makedirs(paths.ready, exist_ok=True)
    os.makedirs(paths.archive, exist_ok=True)
    names = os.listdir(paths.active) if os.path.isdir(paths.active) else []
    moved = sealable(names, now)
    for name in moved:  # rename en el mismo volumen: atómico para el lector
        shutil.move(os.path.join(paths.active, name), os.path.join(paths.ready, name))
    return moved


def ensure_table(spark: SparkSession, table: str) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            ts               timestamp,
            level            string,
            message          string,
            message_template string,
            exception        string,
            trace_id         string,
            source_context   string,
            request_method   string,
            request_path     string,
            status_code      int,
            elapsed_ms       double,
            raw              string,
            source_file      string,
            run_id           string,
            ingested_at      timestamp
        ) USING iceberg
        PARTITIONED BY (days(ts))
        TBLPROPERTIES ('format-version' = '2')
        """
    )


def ingest(spark: SparkSession, paths: Paths, run_id: str) -> int:
    """Una corrida availableNow. Devuelve numInputRows sumado de los micro-batches."""
    ensure_table(spark, paths.table)
    lines = (
        spark.readStream.format("text")
        .option("cleanSource", "archive")
        .option("sourceArchiveDir", paths.archive)
        .load(paths.ready)
        .select(F.col("value").alias("raw"), F.col("_metadata.file_path").alias("source_file"))
    )
    parsed = lines.withColumn("j", F.from_json("raw", _CLEF)).select(
        F.to_timestamp(F.col("j.`@t`")).alias("ts"),
        F.coalesce(F.col("j.`@l`"), F.lit("Information")).alias("level"),
        F.col("j.`@m`").alias("message"),
        F.col("j.`@mt`").alias("message_template"),
        F.col("j.`@x`").alias("exception"),
        F.col("j.`@tr`").alias("trace_id"),
        F.col("j.SourceContext").alias("source_context"),
        F.col("j.RequestMethod").alias("request_method"),
        F.col("j.RequestPath").alias("request_path"),
        F.col("j.StatusCode").alias("status_code"),
        F.col("j.Elapsed").alias("elapsed_ms"),
        "raw",
        "source_file",
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("ingested_at"),
    )
    query = (
        parsed.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", paths.checkpoint)
        .trigger(availableNow=True)
        .toTable(paths.table)
    )
    query.awaitTermination()
    return sum(int(p.get("numInputRows", 0)) for p in query.recentProgress)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run_id = str(uuid.uuid4())
    paths = Paths()
    moved = seal(paths)
    log.info("run_id=%s sellados=%d %s", run_id, len(moved), moved)
    spark = SparkSession.builder.appName("auditlake-log-ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        rows = ingest(spark, paths, run_id)
        total = spark.table(paths.table).count()
        archived = sorted(f for _, _, fs in os.walk(paths.archive) for f in fs)
        active = sorted(os.listdir(paths.active)) if os.path.isdir(paths.active) else []
        print(f"LOG_INGEST run_id={run_id} numInputRows={rows} bronze.technical_logs={total} "
              f"ready={sorted(os.listdir(paths.ready))} archivados={len(archived)} activos={active}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
