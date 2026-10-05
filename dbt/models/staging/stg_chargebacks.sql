select chargeback_id, payment_id, reason, amount, chargeback_date, cast(dt as date) as chargeback_day, payment_found
from {{ source('silver', 'chargebacks') }}
