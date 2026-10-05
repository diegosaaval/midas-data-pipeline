select
    r.*,
    case when p.currency = 'USD' then r.amount * {{ var('usd_cop') }} else r.amount end as amount_cop,
    p.amount as payment_amount,
    p.transaction_day as payment_day
from {{ ref('stg_refunds') }} r
left join {{ ref('stg_payments') }} p on r.payment_id = p.payment_id
