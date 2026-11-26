-- Conciliación de corridas exitosas: lo leído se explica completo y cada conflicto terminó en cuarentena.
-- Se excluyen corridas con quarantined NULL (fase 2: los conflictos quedaban pendientes a propósito).
{{ config(severity='error') }}
select *
from {{ source('bronze', 'ingestion_runs') }}
where status = 'succeeded'
  and quarantined is not null
  and (read_count <> inserted + duplicates + conflicts or conflicts <> quarantined)
