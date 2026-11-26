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
- **Archivos pequeños:** `maintenance.py compact` ejecuta rewrite_data_files (strategy sort por event_type,
  occurred_at; min-input-files=2) con el lock de la tabla, así nunca corre a la vez que su escritor. Medido:
  262 → 192 archivos, máximo por partición 25 → 1, y los pares (event_id, payload_hash) del snapshot anterior
  y del actual son idénticos (37.695 = 37.695, 0 faltantes, 0 sobrantes). La búsqueda de un lote normal pasó
  de 9 a 2 archivos escaneados. rewrite_manifests solo corre si el conteo de manifests supera el umbral
  configurado (observado: 10 manifests ≤ 50, se omitió).
- **Coordinación de escritores:** un sp_getapplock por tabla. `auditlake:audit_events`: ingestor, backfill,
  purga del histórico y mantenimiento de audit_events. `auditlake:technical_logs`: log_ingest y la
  retención de technical_logs. Quien no obtiene el lock sale sin escribir.
- **ACID:** commits por snapshot, único escritor, correcciones como eventos nuevos que referencian al
  original (nunca UPDATE), time travel como evidencia.
- **Retención:** plazos configurables en conf/local/maintenance.json (audit_events 365 días en la demo,
  technical_logs 30). DELETE de vencidos sin bloqueo legal → expire_snapshots → remove_orphan_files. El
  DELETE solo publica un snapshot nuevo: lo borrado sigue legible con time travel hasta que expire_snapshots
  elimina los snapshots que lo contenían (medido: el vencido aparece con VERSION AS OF el snapshot previo
  al DELETE y, tras expirar, la consulta falla con "Cannot find snapshot"). Expirar snapshots por sí solo no
  elimina eventos vigentes. Los bloqueos legales están en conf/ solo para la PoC; en producción vendrían de
  una fuente gobernada (sistema de casos legales o tabla con aprobación y auditoría), leída por el job.
- **Huérfanos:** remove_orphan_files lista la ubicación de la tabla con el FileSystem de Hadoop; la imagen
  trae Hadoop 3.3.4 sin hadoop-aws ("No FileSystem for scheme s3"). En lugar de agregar JARs se usa
  `file_list_view`: el listado real se obtiene con S3FileIO.listPrefix de Iceberg y se pasa al procedimiento
  como vista. Vía operativa: procedimiento con older_than de 3 días y dry_run primero. El procedimiento
  rechaza intervalos menores a 24 h, así que el ensayo con un archivo recién creado usa la Action API (la vía
  que el propio error indica) solo en el namespace sandbox: dry_run con una función de borrado que solo
  registra la ruta, luego la limpieza real; los archivos referenciados y las filas siguen disponibles.
- **Búsqueda de existencia:** el ingestor busca cada event_id en bronze sin filtrar por occurred_at, así un
  evento archivado que llega con otra fecha se clasifica como conflicto y nunca se inserta dos veces. La
  búsqueda usa el rango [min, max] de event_id del lote + semi-join: Iceberg descarta archivos por los
  límites de la columna (métricas `full` para event_id; un IN con más de 200 valores no se poda). Como
  UUIDv7 ordena por tiempo, el rango de un lote toca pocos archivos (medido: 1 de 13). La poda por
  occurred_at solo se aplica en el MERGE de los eventos ya confirmados como nuevos.
- **Backfill (fase 5):** cada evento histórico conserva su fecha original en occurred_at y su event_id es un
  UUIDv7 determinista: 48 bits con los ms de occurred_at y los 74 bits restantes de SHA-256 de
  `sqlserver:AuditLake/legacy.AuditLog/<Id>` (no UUIDv5: la búsqueda por rango depende del orden temporal).
  El payload es v1 canónico (to_json con orden fijo de campos) y payload_hash = sha2(payload, 256). Se
  procesa por meses con la misma búsqueda por rango y MERGE que el ingestor. Medido con 36.800 filas
  (may–oct): corrida 1 inserted=36.800; corrida 2 del mismo rango inserted=0, duplicates=36.800. La
  conciliación recalcula (event_id, payload_hash) desde SQL y la purga vuelve a conciliar justo antes de
  borrar: solo se eliminan filas que todavía coinciden, en lotes de 1.000 con WAITFOR DELAY (una fila
  alterada en SQL tras el backfill quedó sin purgar). Una fila con discrepancia (hash_mismatch o
  missing_in_bronze) permanece en legacy.AuditLog para revisión humana: no se reescribe bronze ni se fuerza la
  purga; se decide caso a caso (en la PoC, la fila Id=12345 quedó en SQL). Tras el backfill, un lote normal siguió escaneando
  pocos archivos (9 de 262 antes de compactar, 2 de 194 después).
- **Hiper-logs (fase 4):** Serilog escribe CLEF (RenderedCompactJsonFormatter) en `active/` del volumen `applogs`, con
  rotación por minuto como configuración de demo (en producción, por hora o por tamaño). `log_ingest` primero
  sella: mueve a `ready/` solo archivos cuyo periodo terminó hace al menos 1 minuto y nunca el activo (el más
  reciente), porque Spark recuerda cada archivo por ruta y no relee lo que se agregue después. Luego
  `readStream` con `trigger(availableNow=True)` y checkpoint persistente (volumen `spark-checkpoints`; en AWS,
  S3) escribe en bronze.technical_logs; una segunda corrida sin archivos nuevos da numInputRows=0.
  `cleanSource=archive` mueve a `archive/` (fuera de `ready/`) los archivos de un batch cuando el siguiente
  batch se confirma; con availableNow eso ocurre en la corrida siguiente que procese datos. Medido: la
  segunda corrida con el mismo checkpoint procesó cero filas (numInputRows=0, bronze sin cambios) y los
  archivos de la corrida 1 se archivaron recién en la corrida 3, la primera posterior que procesó datos. Se
  mantiene `cleanSource=archive`; un archivo ya registrado en el checkpoint no se vuelve a leer aunque siga
  en `ready/`.
- **Silver y gold (dbt, vistas):** silver.audit_events normaliza TransferApproved v1
  (`transferId, amount, approvedBy`, moneda desconocida) y v2 (`transfer.id, transfer.amount.{value,currency},
  approver.userId`). gold.audit_daily agrupa por día, tipo de evento y moneda; el grupo v1 queda con
  currency NULL, se cuenta y su monto total queda en NULL para no sumar monedas distintas. Tests: unique y
  not_null de event_id y la conciliación de corridas como `error`; not_null de transfer_id y amount como
  `warn` con store_failures (tabla Iceberg en dbt_test__audit). Conciliación: en corridas `succeeded` con
  quarantined no nulo, read_count = inserted + duplicates + conflicts y conflicts = quarantined.
  La prueba del payload malformado lo borra de bronze al terminar (DELETE de Iceberg) solo como limpieza
  de la prueba. En operación el original se conserva durante su plazo de retención y una corrección se
  registra como un evento nuevo que referencia al original; bronze no se edita.
- **Evolución de esquema:** identidad estable, campos opcionales, schema_version; silver lee v1 y v2.
- **Calidad (dbt):** unique/not_null en event_id, accepted_values en event_type, test de conciliación
  read_count = inserted + duplicates + conflicts.

## 5. Modos de falla
| Falla | Comportamiento | Recuperación | Pérdida |
|---|---|---|---|
| Worker cae antes/durante la lectura | Nada escrito (no probado: antes de leer no hay escrituras en Iceberg ni en SQL) | Relee el mismo lote | No |
| Cae a mitad del MERGE | Commit Iceberg atómico, no se aplica (no probado: el MERGE publica un snapshot nuevo en un solo commit del catálogo; si no llega a confirmarse, los lectores siguen viendo el anterior) | Reprocesa el lote | No |
| Cae tras commit Iceberg, antes del DELETE | Archivado pero pendiente en SQL | Detecta duplicados idénticos y borra | No, sin duplicados |
| Cae a mitad del DELETE | Lotes parcialmente borrados | Verifica y borra el resto | No |
| Pico con worker caído | La outbox crece | Drena por lotes al volver | No, si SQL tiene espacio |
| SQL sin espacio | Falla el INSERT de la outbox | Falla la operación de negocio (intencional); alerta previa | No |
| MinIO/S3 o catálogo caídos | No puede escribir | Reintentos con backoff | No |
| Dos workers a la vez | El segundo no obtiene `sp_getapplock` y sale antes de leer | Ninguna; el primero termina el lote | No |
| Compactación vs ingesta | Conflicto de commit | Reintento; ventanas separadas | No |
| Payload malformado | Bronze lo guarda como texto | Test dbt en silver alerta | No |
| Mismo event_id, distinto hash | No bloquea el pipeline | `DELETE ... OUTPUT ... INTO audit.OutboxQuarantine` (fila entrante completa + hash archivado + motivo + run_id) en la misma transacción del DELETE verificado; bronze no se toca | No |
| Falla dbt o mantenimiento | La ingesta sigue | Corregir y reejecutar | No |
| Se pierden archivos de hiper-logs | Ruta best effort | Aceptado y documentado | Posible (decisión explícita) |

**Bloqueos en SQL:** línea base sin RCSI. Cuando EF Core crea la base (MigrateAsync / database update sobre
una base inexistente), ejecuta `CREATE DATABASE [x]; IF SERVERPROPERTY('EngineEdition') <> 5 BEGIN ALTER
DATABASE [x] SET READ_COMMITTED_SNAPSHOT ON; END;` (SqlServerMigrationsSqlGenerator, capturado con
`dotnet ef database update --verbose`). La migración `RcsiOffBaseline` ejecuta, fuera de transacción
(`suppressTransaction: true`), `ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT OFF WITH ROLLBACK
IMMEDIATE`; su Down lo vuelve a ON. Verificado: is_read_committed_snapshot_on = 1 tras InitialOutbox y 0 tras
RcsiOffBaseline. Así la línea base queda en OFF hasta la medición de la fase 6. El worker lee con `WITH (READPAST)`: salta las filas de
transacciones abiertas de la API y las toma en la siguiente pasada (caso 101/102). DELETE en lotes de 4000
por PK (bajo la escalada de 5000). `SET LOCK_TIMEOUT 5000; SET DEADLOCK_PRIORITY LOW` solo en las sesiones
del worker (sessionInitStatement de Spark y sus conexiones JDBC); la API no cambia. Purga del histórico
fuera de pico con WAITFOR DELAY.

**Un solo ingestor activo:** al arrancar, antes de leer, el job toma `sp_getapplock` exclusivo con dueño
Session y espera 0 en una conexión que mantiene abierta toda la corrida; si no lo obtiene, sale sin leer ni
escribir. Si el proceso muere, SQL Server libera el lock al cerrar la sesión. Un ingestor colgado conserva
el lock mientras su sesión SQL siga abierta: en local la recuperación es detener el contenedor del job
(`docker stop <contenedor>`); en AWS, el timeout del Glue Job (con MaxConcurrentRuns=1). El atraso se
detecta con la edad del pendiente más antiguo en audit.Outbox.

**Evidencia de la fase 3 (para la medición de la fase 6):**
- Contención: con una transacción de la API abierta, la lectura sin READPAST quedó bloqueada 5,1 s hasta
  el LOCK_TIMEOUT (error 1222); con READPAST no esperó y tomó la fila en la pasada siguiente.
- RCSI: EF Core lo había activado al crear la base; la línea base quedó en OFF de forma explícita.
- MinIO caído, primera medición: la corrida falló a los 346 s. El tiempo se iba en el servidor REST, que
  al cargar la tabla reintenta leer el metadata.json en S3 (hasta 20 reintentos con backoff de hasta ~5 s,
  más los reintentos del SDK de AWS), mientras el cliente esperaba: ~170 s hasta el timeout de socket del
  primer GET (CREATE TABLE IF NOT EXISTS) y ~156 s más en el segundo GET (registro de la corrida fallida),
  hasta que el servidor se rindió con UnknownHostException.
- Ajuste en conf/local/spark-defaults.conf: `rest.client.connection-timeout-ms=5000`,
  `rest.client.socket-timeout-ms=20000`, `rest.client.max-retries=2` y, para lecturas directas de S3,
  `s3.retry.num-retries=2`, `s3.retry.min-wait-ms=500`, `s3.retry.max-wait-ms=2000`. Segunda medición:
  indisponibilidad detectada a los 24 s del primer acceso al catálogo (53 s de contenedor en total, que
  incluyen el arranque de Spark y el intento de registrar la corrida fallida); las 5 filas siguieron
  pendientes y el reintento las archivó (inserted=5, deleted=5, 0 duplicados en la conciliación por event_id).
  En AWS los valores se revisan aparte en conf/aws (Glue Catalog y S3 tienen otra latencia).
- Archivos huérfanos: no se observaron (39 .parquet en MinIO = 39 referenciados por snapshots) porque el
  fallo ocurrió antes de escribir datos: la corrida no pasó de cargar la tabla. Una ejecución que alcance a
  escribir archivos de datos y falle antes de publicar el snapshot sí puede dejarlos sin referencia; su
  limpieza (remove_orphan_files con un margen de antigüedad seguro) se implementó en la fase 5 (ver
  Huérfanos).

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

## 8. Versiones y runtime

Versiones que funcionan en Docker (verificadas en la PoC):

| Componente | Versión |
|---|---|
| .NET SDK / EF Core | 10.0.404 / 10.0.15; Serilog.AspNetCore 11.0.0 |
| SQL Server | mcr.microsoft.com/mssql/server:2022-CU20-ubuntu-22.04 |
| Spark (imagen apache/spark) | 3.5.4, Scala 2.12, Java 11, Python 3.8.10 |
| Iceberg cliente / REST | iceberg-spark-runtime-3.5_2.12 1.7.1 + iceberg-aws-bundle 1.7.1 / apache/iceberg-rest-fixture 1.8.1 (la primera publicada) |
| JDBC | mssql-jdbc 12.8.1.jre11 |
| dbt | dbt-core 1.8.7, dbt-spark 1.8.0 (sin el extra `[session]`, que instala otra pyspark), dbt-adapters 1.7.0, dbt-common 1.11.0, isodate 0.6.1 (pin por conflicto con dbt-common) |
| MinIO / aws-cli | compilado desde RELEASE.2025-10-15T17-29-55Z / amazon/aws-cli 2.22.35 |

dbt 1.8 es la última línea que soporta Python 3.8, que es el Python de la imagen de Spark 3.5.4.
Validado antes de escribir los modelos: `method: session` usa la SparkSession del proceso (catálogo `lake`), las
vistas quedan registradas en el catálogo REST como vistas Iceberg y store_failures crea tablas Iceberg en S3.

**Runtime objetivo en AWS:** el job PySpark (main, log_ingest, maintenance) apunta a AWS Glue 5.0 (Spark 3.5.4,
Iceberg 1.7.1, Python 3.11). El código se mantiene compatible con Python 3.8 (ruff target py38) para correr igual
en local y en Glue 5. En AWS solo cambia conf/aws: GlueCatalog en lugar de REST, S3 en lugar de MinIO y el
checkpoint de log_ingest en S3. Job con MaxConcurrentRuns=1 y timeout (ver ingestor único).

**dbt en AWS (no implementado):** usar el adaptador dbt-glue (perfil `type: glue`, sesiones interactivas de
Glue con Iceberg y Glue Data Catalog), para que los modelos sigan en Spark SQL sin cambios (get_json_object,
vistas). dbt se ejecutaría desde un contenedor (tarea de ECS o CodeBuild) disparado por EventBridge después del
job de ingesta, con `dbt build` y el mismo proyecto. Las vistas Spark registradas en Glue no se consultan desde
Athena: si negocio necesita gold desde Athena, se materializa como tabla o se usa dbt-athena con el SQL
adaptado a Trino.
