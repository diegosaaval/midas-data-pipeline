select payment_id, txn_count_7d, amount_7d, avg_amount_7d, seconds_since_prev, high_velocity, amount_spike
from {{ source('silver', 'payment_features') }}
