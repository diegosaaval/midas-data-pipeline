-- Un pago no puede actualizarse antes de haber ocurrido.
select * from {{ ref('fct_payments') }}
where updated_at < transaction_date
