select r.*, p.amount as payment_amount, p.transaction_day as payment_day
from {{ ref('stg_refunds') }} r
left join {{ ref('stg_payments') }} p on r.payment_id = p.payment_id
