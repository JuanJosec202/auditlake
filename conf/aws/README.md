# conf/aws: referencia para ejecutar el job en AWS Glue (NO EJECUTADA)

El código de `pipelines/auditlake` es el mismo que corre en Docker. En AWS cambian el catálogo (Glue Data
Catalog en lugar de Iceberg REST) y el almacenamiento (S3 en lugar de MinIO). Estos archivos son una
configuración de referencia: **no se desplegaron ni se ejecutaron en una cuenta de AWS**.

| Archivo | Uso |
|---|---|
| `spark-defaults.conf` | Propiedades de Spark/Iceberg para GlueCatalog + S3 (spark-submit en EMR o prueba manual) |
| `glue_entry.py` | Script de entrada del job (sintaxis validada, sin ejecutar en Glue) |
| `glue-job.json` | Definición del Glue Job (Glue 5.0, `MaxConcurrentRuns=1`, timeout, conexión a SQL Server) para `aws glue create-job --cli-input-json` |

## Pasos para ejecutar el job existente en Glue

1. **Bucket y rol:** un bucket S3 para el warehouse (`s3://<bucket>/warehouse/`) y los artefactos, y un rol IAM
   para Glue con acceso a ese bucket, a Glue Data Catalog y al secreto de SQL Server.
2. **Conexión a SQL Server:** una Glue Connection de tipo JDBC/red en la VPC del SQL Server (RDS o EC2), y las
   credenciales en AWS Secrets Manager (`auditlake/sqlserver` con `username` y `password`).
3. **Artefactos:**
   ```bash
   cd pipelines && zip -r ../auditlake.zip auditlake && cd ..
   aws s3 cp auditlake.zip s3://<bucket>/jobs/auditlake.zip
   aws s3 cp mssql-jdbc-12.8.1.jre11.jar s3://<bucket>/jars/   # el mismo JAR que usa docker/spark/Dockerfile
   aws s3 cp conf/aws/glue_entry.py s3://<bucket>/jobs/glue_entry.py
   ```
4. **Script de entrada ([`glue_entry.py`](glue_entry.py)):** el job lee su configuración de variables de entorno
   (`auditlake/config.py`) y Glue entrega parámetros `--CLAVE`. El script traslada los parámetros y las
   credenciales de Secrets Manager a variables de entorno y llama al mismo `main` que corre en Docker. Solo se
   validó su sintaxis (`python -m py_compile` y `ruff check`); no se ejecutó en Glue.
5. **Job:** `aws glue create-job --cli-input-json file://conf/aws/glue-job.json` (reemplazando los marcadores) y
   un trigger programado de Glue con la frecuencia elegida (ver la sensibilidad 5/15/60 min en
   `docs/design.md` §9). `MaxConcurrentRuns=1` evita corridas solapadas; el `sp_getapplock` por tabla sigue
   coordinando el ingestor, el backfill y el mantenimiento. El timeout del job es la recuperación ante un
   ingestor colgado.
6. **Mantenimiento y backfill:** el mismo patrón con otro script de entrada que llame a `auditlake.maintenance`
   o a `main --backfill`.

## Qué se validó y qué no

| Validado en Docker (local) | Sin ejecutar en AWS |
|---|---|
| El job PySpark completo (ingestor, cuarentena, backfill, conciliación, purga, mantenimiento) con Spark 3.5.4 e Iceberg 1.7.1, la misma combinación de Glue 5.0 | Crear el Glue Job, el trigger, la conexión y el rol |
| Catálogo Iceberg REST y almacenamiento S3 compatible (MinIO) con `S3FileIO` | GlueCatalog contra Glue Data Catalog real y S3 real |
| Driver JDBC de SQL Server, `sp_getapplock` y lectura con READPAST | Conectividad de red Glue → SQL Server y lectura del secreto |
| dbt con `dbt-spark` `method: session` | dbt en AWS (dbt-glue, ver README §10) |
| | `log_ingest` en Glue: sus rutas (`/data/logs`, `/data/checkpoints`) están fijas en `Paths` y en AWS deberían apuntar a S3 |
| | Revalidar en Glue 5 la lectura vectorizada de Iceberg (desactivada por el SIGSEGV local) |
