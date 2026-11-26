#!/usr/bin/env bash
# Orquestador del benchmark de la fase 6 (desde la raíz del repo; Git Bash o Linux).
#   pipelines/benchmarks/run.sh setup | baseline | sql | iceberg | legacy-table | rcsi <off|on> <hint> | storage
# Proyecto aislado auditlake-bench con los límites de docker-compose.bench.yml.
set -euo pipefail
export MSYS_NO_PATHCONV=1
P="docker compose -p auditlake-bench -f docker-compose.yml -f docker-compose.bench.yml"
B="$P run --rm -T spark python /app/benchmarks/bench.py"
RES=pipelines/benchmarks/results
mkdir -p "$RES"
PW=$(grep '^MSSQL_SA_PASSWORD=' .env | cut -d= -f2-)
sqlq(){ $P exec -T sqlserver /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$PW" -d AuditLake -W "$@"; }
go(){ rm -f "$RES/STOP"; }
halt(){ touch "$RES/STOP"; }

case "${1:-}" in
  setup)
    $P up -d --build sqlserver api minio iceberg-rest
    until $P logs api 2>/dev/null | grep -q "Now listening"; do sleep 3; done
    sqlq -i /dev/stdin < sql/seed_legacy_bench.sql
    $P run --rm -T spark python -m auditlake.main --backfill --from 2026-05-01 --to 2026-11-01
    $P run --rm -T spark sh -c "cd /dbt && dbt build --profiles-dir /dbt --quiet"
    $P run --rm -T spark python -m auditlake.maintenance compact --table bronze.audit_events ;;
  baseline)
    go; $B approve --label baseline ;;
  sql)            # histórico consultado en SQL Server mientras se aprueban transferencias
    go; $B query-sql --label sql & q=$!; sleep 20
    $B approve --label sql-history; halt; wait $q ;;
  iceberg)        # misma consulta en Iceberg (Spark) con el ingestor activo
    $P restart iceberg-rest >/dev/null; sleep 8
    go; $B query-iceberg --label iceberg & q=$!; $B ingest-loop --label iceberg & i=$!; sleep 45
    $B approve --label iceberg-history; halt; wait $q $i ;;
  legacy-table)   # complementario: auditoría escrita en tabla SQL (legacy.AuditLog) en vez de outbox
    AUDIT_SINK=LegacyTable $P up -d api; sleep 8
    go; $B approve --label legacy-table
    AUDIT_SINK=Outbox $P up -d api; sleep 8 ;;
  rcsi)           # rcsi <off|on> <READPAST|READCOMMITTEDLOCK, READPAST|none>
    state=$(echo "$2" | tr a-z A-Z); hint="$3"; [ "$hint" = none ] && hint=""
    label="rcsi-$2-$(echo "${3}" | tr -c 'a-zA-Z' '_' | tr A-Z a-z)"
    sqlq -Q "ALTER DATABASE AuditLake SET READ_COMMITTED_SNAPSHOT $state WITH ROLLBACK IMMEDIATE"
    # El catálogo local (iceberg-rest-fixture con SQLite) puede quedar con un lock tomado tras un commit fallido.
    $P restart iceberg-rest >/dev/null; sleep 8
    go; $B waits --label "$label" & w=$!
    OUTBOX_READ_HINT="$hint" $B ingest-loop --label "$label" & i=$!; sleep 30
    $B approve --label "$label"; halt; wait $w $i ;;
  storage)
    sqlq -Q "EXEC sp_spaceused N'legacy.AuditLog'" ;;
  *) echo "uso: $0 setup|baseline|sql|iceberg|legacy-table|rcsi <off|on> <hint>|storage"; exit 2 ;;
esac
