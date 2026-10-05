-- KPIs diarios del negocio: lo que mira el comité financiero cada mañana.
with pagos as (
    select
        transaction_day as fecha,
        count(*) as pagos,
        sum(case when status = 'approved' then 1 else 0 end) as pagos_aprobados,
        sum(case when status = 'approved' then amount_cop else 0 end) as tpv_cop,
        count(distinct customer_id) as clientes_activos
    from {{ ref('fct_payments') }}
    group by 1
),
devoluciones as (
    select refund_day as fecha, count(*) as devoluciones, sum(amount_cop) as devoluciones_cop
    from {{ ref('fct_refunds') }}
    group by 1
),
contracargos as (
    select chargeback_day as fecha, count(*) as contracargos, sum(amount_cop) as contracargos_cop
    from {{ ref('fct_chargebacks') }}
    group by 1
)
select
    p.fecha,
    p.pagos,
    p.pagos_aprobados,
    cast(p.pagos_aprobados as double) / nullif(p.pagos, 0) as tasa_aprobacion,
    p.tpv_cop,
    p.tpv_cop / nullif(p.pagos_aprobados, 0) as ticket_promedio_cop,
    p.clientes_activos,
    coalesce(d.devoluciones, 0) as devoluciones,
    coalesce(d.devoluciones_cop, 0) as devoluciones_cop,
    coalesce(d.devoluciones_cop, 0) / nullif(p.tpv_cop, 0) as tasa_devolucion,
    coalesce(c.contracargos, 0) as contracargos,
    coalesce(c.contracargos_cop, 0) / nullif(p.tpv_cop, 0) as tasa_contracargos
from pagos p
left join devoluciones d on p.fecha = d.fecha
left join contracargos c on p.fecha = c.fecha
