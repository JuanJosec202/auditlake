-- Conciliación de corridas exitosas: lo leído se explica completo y cada conflicto terminó en cuarentena.
-- conflicts = quarantined solo aplica a corridas de la outbox con cuarentena (quarantined no nulo): en la fase 2
-- los conflictos quedaban pendientes a propósito y el backfill no usa cuarentena.
{{ config(severity='error') }}
select *
from {{ source('bronze', 'ingestion_runs') }}
where status = 'succeeded'
  and (read_count <> inserted + duplicates + conflicts
       or (quarantined is not null and conflicts <> quarantined))
