select merchant_id, name as merchant_name, category, city, status, updated_at
from {{ source('silver', 'merchants') }}
