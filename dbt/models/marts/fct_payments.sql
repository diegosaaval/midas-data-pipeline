{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}
-- Incremental con ventana de reproceso: cada corrida recalcula los últimos N días para
-- incorporar pagos tardíos y cambios de estado, sin reconstruir toda la historia.
select
    p.payment_id,
    p.customer_id,
    p.merchant_id,
    m.category as merchant_category,
    c.risk_segment,
    p.payment_method,
    p.channel,
    p.status,
    p.currency,
    p.amount,
    case when p.currency = 'USD' then p.amount * {{ var('usd_cop') }} else p.amount end as amount_cop,
    p.transaction_date,
    p.transaction_day,
    coalesce(f.high_velocity, false) as flag_high_velocity,
    coalesce(f.amount_spike, false) as flag_amount_spike,
    p.updated_at
from {{ ref('stg_payments') }} p
left join {{ ref('dim_merchants') }} m on p.merchant_id = m.merchant_id
left join {{ ref('dim_customers') }} c on p.customer_id = c.customer_id
left join {{ ref('stg_payment_features') }} f on p.payment_id = f.payment_id
{% if is_incremental() %}
where p.transaction_day >= {{ dbt.dateadd('day', -var('late_data_days'), '(select max(transaction_day) from ' ~ this ~ ')') }}
{% endif %}
