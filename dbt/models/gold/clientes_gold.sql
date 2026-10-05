-- Datasets gold publicados para consumo (y monitoreados por ATLAS).
select
    customer_id as id_cliente,
    document_type as tipo_documento,
    document_number as numero_documento,
    city as ciudad,
    risk_segment as segmento_riesgo,
    cast(created_at as date) as fecha_vinculacion,
    updated_at as actualizado_en
from {{ ref('dim_customers') }}
