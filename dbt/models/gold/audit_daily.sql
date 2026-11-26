-- Vista diaria por tipo de evento y moneda. Los eventos v1 (moneda desconocida) quedan en su propio
-- grupo con currency = NULL: se cuentan, pero su monto total queda en NULL para no sumar monedas distintas.
select
    cast(occurred_at as date)                                     as day,
    event_type,
    currency,
    count(*)                                                      as events,
    case when currency is null then null else sum(amount) end     as total_amount,
    count(distinct approved_by)                                   as approvers
from {{ ref('audit_events') }}
group by cast(occurred_at as date), event_type, currency
