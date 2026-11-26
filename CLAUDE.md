# AuditLake: Offloading de auditoría a Lakehouse (PoC)

Prueba de concepto: sacar la auditoría y los hiper-logs del core .NET 10 / SQL Server hacia un
Lakehouse (PySpark + Apache Iceberg + dbt + catálogo compatible con AWS Glue), dockerizado localmente.

Antes de planear cualquier fase, lee también docs/design.md: contiene los porqués de cada decisión,
los modos de falla, las pruebas de aceptación y la estructura del README.

## Alcance congelado. No agregar componentes fuera de esta lista.
- Docker Compose: sqlserver, api, minio (+ minio-init), iceberg-rest, spark
- .NET 10 (EF Core 10): un proyecto API (carpetas Domain/Application/Infrastructure) + un proyecto de tests
- SQL Server: audit.Outbox (sin columna de estado), audit.OutboxQuarantine, legacy.AuditLog; RCSI OFF como línea base (se decide en la fase 6)
- Python: outbox_source, audit_sink, log_ingest, main (--backfill), maintenance.py; conf/local y conf/aws
- Iceberg: bronze.audit_events, bronze.technical_logs, bronze.ingestion_runs
- dbt (dbt-spark, method: session): silver.audit_events, gold.audit_daily (vistas) + tests
- Sin Makefile: el punto de entrada es Docker Compose (base sin puertos; docker-compose.dev.yml para desarrollo;
  demo: docker compose -p auditlake-demo -f docker-compose.yml --env-file .env.example run --build --rm spark python -m auditlake.demo)

## Fuera de alcance: no proponer
Kafka, Debezium/CDC, Airflow, MediatR/CQRS, repositorio genérico sobre EF, varios proyectos .NET por capa,
Lake Formation local, buffer asíncrono en memoria, gold incremental, imagen de Glue dentro del Compose.

## Flujos
1. Auditoría obligatoria (cero pérdida): negocio + audit.Outbox en la misma transacción → job PySpark
   → MERGE idempotente en bronze.audit_events → verificación de hash → DELETE en la outbox.
2. Hiper-logs (best effort): Serilog JSON a archivos → log_ingest (readStream, availableNow, checkpoint)
   → bronze.technical_logs. Nunca pasan por SQL Server.
3. Histórico (una vez): legacy.AuditLog → main --backfill (event_id determinista desde la PK)
   → conciliación por rango → purga en SQL por lotes.

## Reglas técnicas
- .NET: un solo SaveChangesAsync para negocio + outbox. EventId = UUIDv7 (Guid.CreateVersion7()). PayloadHash = SHA-256 del payload canónico.
- Outbox: PK bigint IDENTITY clusterizada; la presencia de la fila significa "pendiente".
- Worker en SQL: SET LOCK_TIMEOUT 5000; SET DEADLOCK_PRIORITY LOW solo en sus sesiones; DELETE en lotes de 4000 por PK.
- Un escritor por tabla con sp_getapplock exclusivo (Session, timeout 0): `auditlake:audit_events` (ingestor, backfill,
  purga y mantenimiento de audit_events) y `auditlake:technical_logs` (log_ingest y su retención). Quien no lo obtiene sale.
- Lectura de la outbox: SELECT TOP (n) ... WITH (READPAST) ORDER BY Id sobre filas existentes. Nunca watermark por ID máximo.
- Lote materializado antes de escribir (cache + count) para evitar re-evaluación JDBC.
- Búsqueda de existencia por rango de event_id + semi-join, sin filtrar por occurred_at (métricas full de event_id).
- MERGE Iceberg solo WHEN NOT MATCHED THEN INSERT de los eventos confirmados como nuevos, con poda por su rango de occurred_at.
- Conflicto (mismo event_id, distinto hash): mover la fila a audit.OutboxQuarantine en la misma transacción del DELETE
  (DELETE ... OUTPUT con columnas explícitas ... INTO); se cuenta en ingestion_runs.quarantined.
- Bronze guarda el payload como texto; el parseo y la normalización v1/v2 ocurren en silver (dbt).
- Particionamiento: days(occurred_at) oculto; orden de escritura event_type, occurred_at.
- Mantenimiento con procedimientos de Iceberg (rewrite_data_files, rewrite_manifests, expire_snapshots,
  remove_orphan_files). No usar sintaxis de Delta (OPTIMIZE, VACUUM).
- Un único escritor por tabla; la compactación no corre a la vez que la ingesta.
- Catálogo: Iceberg REST en local; GlueCatalog en conf/aws. Mismo código, solo cambia la configuración.
- Prueba de caída: variable FAIL_AFTER_ICEBERG_COMMIT=1 lanza una excepción entre el commit Iceberg y el DELETE.

## Buenas prácticas
- SOLID sin ceremonia: el caso de uso usa AuditLakeDbContext directamente (sin repositorios ni IAuditTrail),
  decide qué se audita y agrega OutboxMessage.Create(evento) al contexto; OutboxMessage serializa el payload
  canónico y calcula el hash. Un solo SaveChangesAsync al final del caso de uso.
- Nombres de negocio (TransferApproved, no LogEntry). TimeProvider para fechas. IOptions para configuración.
- Python: lógica en funciones puras (p. ej. classify), I/O en los bordes, type hints, ruff, logs con run_id.
- Tests: atomicidad de la outbox, orden de commit 101/102 (Testcontainers), classify, tests dbt.

## Flujo de trabajo
- Proponer un plan antes de implementar cada fase. No avanzar de fase sin confirmación.
- Ejecutar los comandos y tests y mostrar la salida; no dar por hecho que algo "debería funcionar".
- Si algo del alcance no funciona en la práctica (versiones, imágenes, JARs), avisar y proponer la alternativa;
  no cambiarlo en silencio.
- Cada fase termina con tests en verde y un commit pequeño.
- Cuando el usuario corrija una propuesta, recordarle registrarla en docs/ai-log.md.

## Fases
1. Compose (sqlserver, api) + API .NET 10 con outbox (DbContext directo) + 2 tests de atomicidad.
2. minio, minio-init, iceberg-rest + servicio spark + job de ingesta (outbox_source, audit_sink, main) + ingestion_runs + prueba de caída.
3. Cuarentena + READPAST y ajustes de sesión del worker + test 101/102 + ingestor único (sp_getapplock) + caídas (tras commit Iceberg, MinIO caído) + main --reconcile. Sin RCSI.
4. Serilog + log_ingest + proyecto dbt (silver, gold, tests, conciliación) + evento v2.
5. legacy.AuditLog + --backfill (UUIDv7 determinista) + purga verificada + maintenance.py (compactación, manifests
   condicionado, retención con bloqueo legal, expire_snapshots, huérfanos con file_list_view).
6. Benchmark p95 y tamaños + comparación con y sin RCSI (contención medida, usando la evidencia de la fase 3 en docs/design.md):
   validar la lectura del worker con RCSI activo; si se quiere conservar el comportamiento de READPAST,
   probar la variante WITH (READCOMMITTEDLOCK, READPAST) + demo con un comando de Compose + README.
