select
    customer_id,
    document_type,
    document_number,
    city,
    risk_segment,
    created_at,
    updated_at
from {{ source('silver', 'customers') }}
