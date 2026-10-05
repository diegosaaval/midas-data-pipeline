select refund_id, payment_id, amount, refund_date, cast(dt as date) as refund_day, payment_found
from {{ source('silver', 'refunds') }}
