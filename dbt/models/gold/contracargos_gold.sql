select
    chargeback_id as id_contracargo,
    payment_id as id_pago,
    merchant_id as id_comercio,
    customer_id as id_cliente,
    reason as motivo,
    amount as monto,
    chargeback_day as fecha,
    payment_day as fecha_pago,
    days_to_chargeback as dias_hasta_contracargo
from {{ ref('fct_chargebacks') }}
