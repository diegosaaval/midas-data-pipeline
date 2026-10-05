select
    payment_id,
    customer_id,
    merchant_id,
    amount,
    coalesce(currency, 'COP') as currency,
    payment_method,
    channel,
    status,
    transaction_date,
    cast(dt as date) as transaction_day,
    updated_at
from {{ source('silver', 'payments') }}
