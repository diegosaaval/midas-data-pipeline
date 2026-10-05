select
    payment_id as id_pago,
    customer_id as id_cliente,
    merchant_id as id_comercio,
    merchant_category as categoria_comercio,
    payment_method as medio_pago,
    channel as canal,
    status as estado,
    currency as moneda,
    amount as monto,
    amount_cop as monto_cop,
    transaction_date as fecha_transaccion,
    transaction_day as fecha,
    flag_high_velocity as alerta_velocidad,
    flag_amount_spike as alerta_monto
from {{ ref('fct_payments') }}
