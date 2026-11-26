-- Normaliza TransferApproved v1 y v2 a un mismo esquema. Bronze guarda el payload como texto;
-- un payload malformado deja campos nulos y lo detectan los tests (warn + store_failures).
--   v1: {"transferId","amount","approvedBy","approvedAt"}                       (moneda desconocida)
--   v2: {"transfer":{"id","amount":{"value","currency"}},"approver":{"userId"},"approvedAt"}
with src as (
    select * from {{ source('bronze', 'audit_events') }}
)
select
    event_id,
    event_type,
    schema_version,
    occurred_at,
    case schema_version
        when 1 then get_json_object(payload, '$.transferId')
        when 2 then get_json_object(payload, '$.transfer.id')
    end as transfer_id,
    cast(case schema_version
        when 1 then get_json_object(payload, '$.amount')
        when 2 then get_json_object(payload, '$.transfer.amount.value')
    end as decimal(18, 2)) as amount,
    case schema_version
        when 2 then get_json_object(payload, '$.transfer.amount.currency')
    end as currency,
    case schema_version
        when 1 then get_json_object(payload, '$.approvedBy')
        when 2 then get_json_object(payload, '$.approver.userId')
    end as approved_by,
    cast(get_json_object(payload, '$.approvedAt') as timestamp) as approved_at,
    payload_hash,
    run_id,
    ingested_at
from src
