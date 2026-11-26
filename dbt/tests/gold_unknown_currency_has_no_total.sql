{{ config(severity='error') }}
select * from {{ ref('audit_daily') }} where currency is null and total_amount is not null
