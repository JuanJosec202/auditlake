# Bitácora de uso de IA

Registro de cómo se usaron los asistentes de IA en AuditLake: qué aceleraron, dónde fallaron y qué
decidí yo. Solo casos reales, cada uno con su evidencia (commit, prueba o medición).

## Cómo orquesté los asistentes

| Asistente | Rol | Cómo lo controlé |
|---|---|---|
| Claude (chat) | Revisor de arquitectura: evaluar la propuesta, revisar cada plan y cada resultado de Claude Code | Sus sugerencias también se revisaron; varias se descartaron (ver entradas 6 y 14) |
| Claude Code | Implementación por fases en el repositorio | `CLAUDE.md` con alcance congelado, plan obligatorio antes de cada fase, salida real de pruebas como condición de cierre |
| ChatGPT | Contraste de los mensajes de aprobación, segunda opinión técnica y consulta de documentación oficial | Lo que aportó se validó contra la documentación y contra pruebas |

Reglas que impuse desde el inicio:
- **Alcance congelado en `CLAUDE.md`**, con una lista explícita de lo que no se debe proponer (Kafka, Debezium, Airflow, MediatR/CQRS, etc.).
- **Ninguna fase se implementa sin plan aprobado**, y ninguna se cierra sin la salida real de las pruebas.
- **No se acepta un "debería funcionar"**: versiones, imágenes y JARs se validan ejecutándolos.

---

## Entradas

### 1. Diseño: revisión de la propuesta inicial
- **Herramienta:** Claude (chat)
- **Prompt:** evaluar una propuesta inicial (outbox + PySpark + Iceberg + dbt) contra el enunciado y la rúbrica.
- **Respuesta de la IA:** validó el enfoque y señaló huecos: los hiper-logs no tenían ruta, la outbox costaba tres escrituras por evento, el MERGE no podaba particiones, Glue Catalog no corre en Docker y el texto de la propuesta tenía restos de citas generadas.
- **Tipo:** acierto
- **Decisión humana:** congelé el alcance en `CLAUDE.md` y recorté lo que no aportaba (tabla de conflictos aparte, imagen de Glue en el Compose, gold incremental, ventana caliente en SQL).
- **Evidencia:** `CLAUDE.md`, `docs/design.md`
- **Aceleró:** convirtió una propuesta conceptual en un alcance implementable por fases.

### 2. Fase 1: SDK 9 con `global.json` para compilar net8.0
- **Herramienta:** Claude Code
- **Respuesta de la IA:** como no había SDK de .NET 8 instalado, propuso compilar `net8.0` con el SDK 9 fijado en `global.json` y un generador propio de UUIDv7 (`Guid.CreateVersion7` no existe en .NET 8).
- **Tipo:** ineficiente
- **Problema:** era un parche de entorno, sumaba código propio para UUIDv7 y dejaba una combinación de versiones difícil de explicar.
- **Decisión humana:** pasar a .NET 10 LTS y EF Core 10. Cumple el ".NET 8+" del enunciado, es LTS y trae `Guid.CreateVersion7()` nativo.
- **Evidencia:** commit `1f7b352`

### 3. Fase 1: repositorio y unidad de trabajo sobre EF Core
- **Herramienta:** Claude Code
- **Respuesta de la IA:** el plan incluía `ITransferRepository` e `IUnitOfWork` sobre EF Core.
- **Tipo:** sobreingeniería
- **Problema:** el `DbContext` ya es unidad de trabajo y repositorio. Eran capas de indirección sin beneficio para la PoC.
- **Decisión humana:** usar el `DbContext` directamente y mantener la serialización y el hash en un solo lugar (`OutboxMessage.Create`).
- **Evidencia:** commits `1f7b352` y `b40a390`; prueba de atomicidad con un CHECK real que revierte la aprobación.

### 4. Fase 2: imagen y persistencia del catálogo REST
- **Herramienta:** Claude Code
- **Respuesta de la IA:** detectó que la imagen de tabulario está abandonada, propuso `apache/iceberg-rest-fixture` y avisó que su catálogo vive en memoria por defecto. Propuso persistirlo con SQLite en un volumen.
- **Tipo:** acierto
- **Decisión humana:** como la imagen es un fixture de pruebas, exigí una prueba que demostrara que la tabla sigue consultable después de reiniciar el catálogo.
- **Evidencia:** conteo y snapshot idénticos antes y después de `docker compose restart iceberg-rest`; commit `a17e012`.

### 5. Fase 2: mi propuesta de detener la corrida ante un conflicto
- **Herramienta:** revisión con Claude (chat)
- **Propuesta (mía):** si llega el mismo `event_id` con otro hash, detener la corrida y dejar la fila en SQL.
- **Tipo:** error de diseño propio, detectado en revisión
- **Problema:** como la lectura es `ORDER BY Id`, una sola fila en conflicto bloquearía el drenaje de toda la outbox para siempre (poison message), y SQL volvería a acumular auditoría.
- **Decisión humana:** contar el conflicto y continuar con el resto del lote; en la Fase 3, mover la fila a `audit.OutboxQuarantine`.
- **Evidencia:** `test_conflict_stays_pending_and_run_continues` (`inserted=1, conflicts=1, deleted=1`).

### 6. Fase 2: búsqueda por `event_id` y poda por fecha
- **Herramientas:** Claude Code y Claude (chat)
- **Respuesta de la IA:** Claude Code detectó que la poda por `occurred_at` fallaría si un mismo `event_id` llegaba con otra fecha, y se insertaría un duplicado. Claude (chat) propuso una invariante: generar el ID con `Guid.CreateVersion7(occurredAt)`.
- **Tipo:** acierto parcial
- **Problema:** la invariante depende de que todos los productores la cumplan.
- **Decisión humana:** buscar el `event_id` en toda la tabla, sin filtrar por fecha, y usar la poda por fecha solo en el MERGE de los nuevos. Es correcto aunque un productor genere mal un ID. Exigí evidencia de que la búsqueda no lee todos los archivos.
- **Aporte de la IA al implementar:** usar un rango min/max de `event_id` en lugar de `IN`, porque Iceberg no poda por métricas un `IN` de más de 200 valores, y fijar `write.metadata.metrics.column.event_id = full`.
- **Evidencia:** prueba con un ID archivado que llega con otra fecha (1 fila en Iceberg, conflicto reportado); 1 de 13 archivos escaneados; commit `9654059`.

### 7. Fase 3: lectura bloqueada sin READPAST
- **Herramienta:** Claude Code
- **Respuesta de la IA:** encontró que la lectura `SELECT TOP … ORDER BY Id` se bloquea en la fila de una transacción abierta, y propuso `WITH (READPAST)`.
- **Tipo:** acierto
- **Decisión humana:** no activar RCSI por defecto, sino medir primero; guardar la evidencia del bloqueo para decidir en la Fase 6.
- **Evidencia:** `Lock request time out period exceeded` a los 5 s sin el hint; prueba 101/102 con ambos eventos archivados una vez; commit `2881997`.

### 8. Fase 3: RCSI activado por EF Core sin que nadie lo pidiera
- **Herramienta:** Claude Code
- **Hallazgo:** al crear la base, EF Core ejecuta `ALTER DATABASE … SET READ_COMMITTED_SNAPSHOT ON`. RCSI estuvo activo desde la Fase 1 aunque yo había decidido medir antes de activarlo, así que las pruebas de la Fase 2 corrieron en una condición distinta a la que creía.
- **Tipo:** comportamiento implícito del framework que invalidaba una conclusión
- **Decisión humana:** exigí el origen exacto con evidencia antes de documentarlo. La migración `RcsiOffBaseline` fija la línea base en OFF y las pruebas se repitieron.
- **Evidencia:** SQL capturado con `dotnet ef database update --verbose` (`is_read_committed_snapshot_on` pasa de 1 a 0); commit `76abe67`.
- **Candidato a la pregunta 4 del README.**

### 9. Fase 3: un solo ingestor activo
- **Herramientas:** Claude Code, Claude (chat), ChatGPT y la documentación de `sp_getapplock`
- **Decisión humana:** pedí un mecanismo que no dejara locks huérfanos si el proceso muere. La propuesta fue `sp_getapplock` con `@LockOwner='Session'` y `@LockTimeout=0`. Al contrastarla con otro asistente y con la documentación, agregué columnas explícitas en `OUTPUT … INTO` (en lugar de `DELETED.*`) y la variante `READCOMMITTEDLOCK, READPAST` para validar con RCSI activo.
- **Evidencia:** dos arranques simultáneos; uno termina `skipped`. Commit `2881997`.

### 10. Fase 3: la corrida tardaba 346 s en fallar con MinIO caído
- **Herramienta:** Claude Code
- **Decisión humana:** no bajar un timeout a ciegas, sino identificar primero dónde se consumían los 346 s.
- **Respuesta de la IA:** la causa eran unos 20 reintentos con backoff del servidor REST leyendo el `metadata.json`. Ajustó los timeouts y reintentos en `conf/local`.
- **Evidencia:** detección en 24 s; reintento con `inserted=5, deleted=5` y 0 duplicados; commit `76abe67`.

### 11. Fase 4: montos de monedas distintas sumados en gold
- **Herramienta:** Claude Code
- **Respuesta de la IA:** `gold.audit_daily` sumaba el monto total por día y tipo de evento, sin considerar que v2 trae moneda y v1 no.
- **Tipo:** error de datos
- **Decisión humana:** agrupar también por moneda y dejar el total en `NULL` cuando la moneda es desconocida, mostrando el conteo.
- **Evidencia:** gold con COP y `NULL` el mismo día; commit `0f48958`.

### 12. Fase 4: archivado de logs con `cleanSource`
- **Herramienta:** Claude Code
- **Respuesta de la IA:** reportó que Spark archiva los archivos de un batch recién en la siguiente corrida que procesa datos, y ofreció implementar un archivado propio leyendo el checkpoint.
- **Decisión humana:** aceptar el comportamiento de Spark y documentarlo. No se relee nada porque el checkpoint lo registra, y un archivado propio sería código frágil sobre un detalle interno.
- **Evidencia:** corridas 2 y 4 con `numInputRows=0`; commit `265d9b5`.

### 13. Fase 4: metastore Derby en el repositorio
- **Herramienta:** Claude Code
- **Hallazgo:** los archivos del metastore Derby que crea Spark se colaron en un commit. La IA lo detectó y lo corrigió (`.gitignore` y directorio en `/tmp`).
- **Lección:** revisar los diffs antes de hacer push.

### 14. Fase 5: UUIDv5 para el histórico
- **Herramienta:** ChatGPT
- **Respuesta de la IA:** generar el `event_id` del backfill como UUIDv5 a partir de sistema, tabla y PK. La sugerencia apareció dos veces.
- **Tipo:** errónea en este contexto
- **Problema:** un UUIDv5 es un hash sin orden temporal. Los archivos del histórico tendrían rangos de `event_id` que cubren todo el espacio de IDs, y cada lote normal del ingestor tendría que leerlos todos, lo que rompe la poda ya demostrada.
- **Decisión humana:** UUIDv7 determinista: 48 bits con los ms de `occurred_at` y el resto derivado de SHA-256 de sistema, tabla y PK. Es igual de reproducible y conserva el orden temporal.
- **Evidencia:** después del backfill, un lote normal escanea 9 de 262 archivos (2 de 194 tras compactar); el backfill repetido da `inserted=0, duplicates=36800`; commit `9a3c011`.

### 15. Fase 5: JARs de hadoop-aws para limpiar huérfanos
- **Herramienta:** Claude Code
- **Respuesta de la IA:** anticipó que `remove_orphan_files` fallaría sin `hadoop-aws` y propuso agregar esos JARs.
- **Decisión humana:** verificar primero la versión real de Hadoop y probar `file_list_view` de Iceberg.
- **Evidencia:** con `file_list_view` funcionó sin JARs nuevos; dry_run listó el archivo y la limpieza lo borró; commit `9a3c011`.

### 16. Fase 6: qué medir para comparar SQL con Iceberg
- **Herramientas:** Claude (chat) y Claude Code
- **Problema detectado en revisión:** comparar solo la latencia de la consulta histórica favorece a SQL a escala local (el arranque de Spark domina) y no mide el beneficio real de la arquitectura.
- **Decisión humana:** medir el p95 del core mientras corre la consulta histórica, validar que SQL e Iceberg devuelvan los mismos totales y aplicar límites de recursos a todos los servicios, Spark incluido.
- **Evidencia:** p95 de las aprobaciones de 283,6 ms con la consulta en SQL frente a 38,7 ms con Iceberg (28,6 ms sin consulta); mismos totales en los 184 días; commit `2418de3`.

### 17. Fase 6: fallos reales encontrados al medir con 1 millón de filas
- **Herramienta:** Claude Code
- **Hallazgos y correcciones:**
  - La lectura vectorizada de Iceberg tumbaba la JVM (SIGSEGV) con Java 11 y 17: se desactivó en local.
  - El catálogo REST sobre SQLite devolvía 500 en commits concurrentes: el ingestor falla sin borrar de SQL y el reintento no duplica (0 duplicados en 1,08 M de filas).
  - Un `IN` con unos 170.000 IDs agotaba la memoria en el backfill: se reemplazó por un semi-join.
  - Los logs fallaban por permisos en un clon limpio: pasaron a un volumen propio.
- **Lección:** los fallos aparecieron solo con volumen real y en un clon limpio, y el diseño idempotente evitó pérdidas o duplicados.

---

### 18. Redacción de la documentación
- **Herramientas:** Claude (chat) y Claude Code
- **Uso:** esta bitácora, `docs/design.md` y el README se redactaron con IA a partir de los planes, las salidas de las pruebas y las decisiones registradas en cada fase.
- **Control humano:** revisé cada entrada contra lo que ocurrió, confirmé el origen de cada sugerencia y exigí que el README usara solo cifras medidas y solo los casos de esta bitácora, sin inventar ni completar entradas.

---

## Balance

- **Dónde aceleró la IA:** el scaffolding de cada fase, la configuración de Spark, Iceberg y MinIO, las pruebas de integración, la causa raíz de fallos de infraestructura y detalles no obvios (el límite de 200 valores en `IN`, READPAST, `file_list_view`).
- **Dónde fue necesario el criterio humano:** el alcance y los recortes de sobreingeniería, las decisiones de identidad de eventos (entradas 6 y 14), la corrección de datos (entrada 11), la decisión de medir antes de activar RCSI (entradas 7 y 8) y qué medir para demostrar valor de negocio (entrada 16).
