select
    cb.*,
    case when p.currency = 'USD' then cb.amount * {{ var('usd_cop') }} else cb.amount end as amount_cop,
    p.merchant_id,
    p.customer_id,
    p.transaction_day as payment_day,
    {{ dbt.datediff('p.transaction_day', 'cb.chargeback_day', 'day') }} as days_to_chargeback
from {{ ref('stg_chargebacks') }} cb
left join {{ ref('stg_payments') }} p on cb.payment_id = p.payment_id
