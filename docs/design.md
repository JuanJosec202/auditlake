# AuditLake: diseño y decisiones

Documento de referencia para la implementación y el README. El alcance y las reglas están en CLAUDE.md;
aquí están los porqués, los modos de falla, las pruebas de aceptación y la estructura del README.

## 1. Problema de negocio
Las tablas de auditoría e hiper-logs del core (.NET 10 + SQL Server) crecen sin control, degradan el
rendimiento relacional y elevan costos. Objetivo: SQL Server conserva solo minutos de eventos pendientes;
el histórico vive en Iceberg sobre S3, consultable y con retención gobernada, sin perder auditoría.

## 2. Arquitectura: tres flujos
1. **Histórico (una vez):** legacy.AuditLog → `main --backfill` → bronze.audit_events → conciliación
   por rango (conteo + hash) → purga en SQL por lotes.
2. **Auditoría obligatoria (cero pérdida):** negocio + audit.Outbox en la misma transacción → job PySpark
   → MERGE idempotente → verificación de hash → DELETE en la outbox.
3. **Hiper-logs (best effort):** Serilog JSON a archivos → log_ingest (availableNow + checkpoint)
   → bronze.technical_logs. Nunca pasan por SQL.

Capa analítica: dbt sobre bronze → silver.audit_events → gold.audit_daily, con tests.
Catálogo: Iceberg REST en local, Glue Data Catalog en AWS (solo cambia conf/).

## 3. Decisiones y trade-offs
| Decisión | Alternativa descartada | Motivo |
|---|---|---|
| Outbox transaccional | CDC (Debezium) | La app conoce el significado de negocio (actor, operación, correlación); CDC captura filas. CDC solo si varias apps escriben en las mismas tablas |
| Outbox transaccional | Buffer asíncrono en memoria | Pierde eventos si el proceso cae |
| Sin broker | Kafka | Un productor y un consumidor; la outbox ya da durabilidad |
| Outbox sin columna de estado | Estado pending/confirmed | Un INSERT y un DELETE por evento, en lugar de tres escrituras |
| Hiper-logs fuera de SQL | Hiper-logs por la outbox | Trasladaría el problema original al nuevo diseño |
| Un único escritor, sin HA del worker | Workers activos en paralelo | El worker no tiene estado; RPO 0, RTO = reinicio + drenaje |
| MinIO + Iceberg REST local | Glue Catalog en Docker | Glue Catalog no existe fuera de AWS; MinIO da paridad de API S3. La edición comunitaria de MinIO ya no publica imágenes (solo código fuente): se compila desde el tag fijado `RELEASE.2025-10-15T17-29-55Z` (docker/minio). En AWS se usa Amazon S3, solo cambia conf/ |
| dbt-spark method: session | Spark Thrift Server | Un contenedor menos |
| Partición days(occurred_at) | Partición por event_type | Las consultas de auditoría filtran por fecha; event_type va como orden de escritura |
| Silver/gold como vistas | Incremental | Se pasa a incremental solo si una medición lo justifica |

Fuera de alcance: Kafka, Debezium, Airflow, MediatR/CQRS, repositorio genérico, varios proyectos .NET,
Lake Formation local, buffer en memoria, gold incremental. Ventana caliente de auditoría en SQL:
decisión pendiente con negocio, no se implementa.

## 4. Diseño del Lakehouse
- **bronze.audit_events:** Iceberg v2, partición days(occurred_at), orden event_type, occurred_at;
  columnas comunes tipadas + payload (texto JSON) + schema_version + payload_hash.
- **bronze.technical_logs:** partición days(ts), retención 30-90 días.
- **bronze.ingestion_runs:** run_id, read_count, inserted, duplicates, conflicts, deleted.
- **Archivos pequeños:** rewrite_data_files por umbral de archivos por partición + rewrite_manifests,
  fuera de la ventana de ingesta.
- **ACID:** commits por snapshot, único escritor, correcciones como eventos nuevos que referencian al
  original (nunca UPDATE), time travel como evidencia.
- **Retención:** DELETE de vencidos sin bloqueo legal → expire_snapshots → remove_orphan_files.
  Expirar snapshots por sí solo no elimina eventos vigentes.
- **Búsqueda de existencia:** el ingestor busca cada event_id en bronze sin filtrar por occurred_at, así un
  evento archivado que llega con otra fecha se clasifica como conflicto y nunca se inserta dos veces. La
  búsqueda usa el rango [min, max] de event_id del lote + semi-join: Iceberg descarta archivos por los
  límites de la columna (métricas `full` para event_id; un IN con más de 200 valores no se poda). Como
  UUIDv7 ordena por tiempo, el rango de un lote toca pocos archivos (medido: 1 de 13). La poda por
  occurred_at solo se aplica en el MERGE de los eventos ya confirmados como nuevos.
- **Requisitos del backfill (fase 5):** cada evento histórico conserva su fecha original en occurred_at y
  una identidad reproducible derivada de la PK de legacy.AuditLog (reejecutar da el mismo event_id). Para
  que la búsqueda por rango siga podando, el event_id determinista debe empezar por el timestamp original
  (forma UUIDv7/v8 con prefijo de tiempo), no ser un hash aleatorio.
- **Evolución de esquema:** identidad estable, campos opcionales, schema_version; silver lee v1 y v2.
- **Calidad (dbt):** unique/not_null en event_id, accepted_values en event_type, test de conciliación
  read_count = inserted + duplicates + conflicts.

## 5. Modos de falla
| Falla | Comportamiento | Recuperación | Pérdida |
|---|---|---|---|
| Worker cae antes/durante la lectura | Nada escrito | Relee el mismo lote | No |
| Cae a mitad del MERGE | Commit Iceberg atómico, no se aplica | Reprocesa el lote | No |
| Cae tras commit Iceberg, antes del DELETE | Archivado pero pendiente en SQL | Detecta duplicados idénticos y borra | No, sin duplicados |
| Cae a mitad del DELETE | Lotes parcialmente borrados | Verifica y borra el resto | No |
| Pico con worker caído | La outbox crece | Drena por lotes al volver | No, si SQL tiene espacio |
| SQL sin espacio | Falla el INSERT de la outbox | Falla la operación de negocio (intencional); alerta previa | No |
| MinIO/S3 o catálogo caídos | No puede escribir | Reintentos con backoff | No |
| Dos workers a la vez | Conflicto de commit Iceberg | Reintento + MERGE idempotente | No |
| Compactación vs ingesta | Conflicto de commit | Reintento; ventanas separadas | No |
| Payload malformado | Bronze lo guarda como texto | Test dbt en silver alerta | No |
| Mismo event_id, distinto hash | No bloquea el pipeline | Mueve la fila a audit.OutboxQuarantine + alerta | No |
| Falla dbt o mantenimiento | La ingesta sigue | Corregir y reejecutar | No |
| Se pierden archivos de hiper-logs | Ruta best effort | Aceptado y documentado | Posible (decisión explícita) |

**Bloqueos en SQL:** RCSI (los lectores no bloquean), DELETE en lotes de 4000 por PK (bajo la escalada
de 5000), DEADLOCK_PRIORITY LOW en el worker, LOCK_TIMEOUT 5000, transacciones abiertas de la API se
toman en la siguiente pasada (caso 101/102), purga del histórico fuera de pico con WAITFOR DELAY.

**Monitoreo:** edad del pendiente más antiguo (métrica principal), filas en la outbox y espacio libre,
corridas fallidas en ingestion_runs, filas en cuarentena > 0, archivos por partición.

**Respaldo:** backups habituales de SQL (incluyen la outbox); snapshots de Iceberg + versionado de S3 en
AWS; silver/gold se reconstruyen desde bronze con dbt build --full-refresh.

## 6. Pruebas de aceptación
1. Cero pérdida: tras la caída, N generados = N en bronze, 0 duplicados, outbox vacía.
2. Orden de commit: 101 y 102 confirmadas fuera de orden quedan archivadas.
3. Sin bloqueo: la API responde con el ingestor apagado; la outbox crece y luego drena.
4. Rendimiento: p95 escribiendo el log en tabla SQL vs outbox, misma carga.
5. Costo: tamaño en SQL vs Parquet comprimido, aplicado a la fórmula de costo por millón de eventos.
6. Compactación: archivos por partición antes y después.
7. Esquema: silver devuelve eventos v1 y v2.
8. make chaos: caída en cada punto, docker stop minio, dos workers; todos terminan conciliados.

## 7. Estructura del README (fase 6)
1. Resumen para negocio (problema, qué cambia, valor estimado)
2. Cómo levantar el entorno (requisitos, make demo, make chaos)
3. Arquitectura (diagrama de los tres flujos)
4. Decisiones y trade-offs (sección 3) y fuera de alcance
5. Diseño del Lakehouse (sección 4)
6. Resiliencia y modos de falla (sección 5)
7. Pruebas de aceptación con mediciones reales (sección 6)
8. Respuestas a las 4 preguntas:
   1. Anti-patrón: logs append-only compiten por buffer pool, IO, backups, log de transacciones y licencia
      por núcleo. Métrica: costo por millón de eventos retenidos + p95 del core, medidos en la PoC.
   2. PySpark mueve datos y garantiza entrega (idempotencia, acuse, compactación); dbt define significado
      y calidad; Glue Catalog es el registro único que leen Athena/EMR/Redshift, con Lake Formation encima,
      sin rehacer el modelo.
   3. Durable desde el commit de negocio; replay = filas pendientes en la outbox; duplicados absorbidos
      por MERGE idempotente + hash; monitoreo de edad del pendiente y espacio.
   4. Casos reales de docs/ai-log.md.
9. Uso de IA (desde docs/ai-log.md: prompts clave, aceleraciones, correcciones humanas con evidencia)
10. Despliegue en AWS (conf/aws, Glue Job con MaxConcurrentRuns=1, Glue Catalog, S3)
