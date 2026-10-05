select * from {{ ref('indicadores_financieros') }}
where tasa_aprobacion not between 0 and 1
   or tasa_devolucion not between 0 and 1
   or tasa_contracargos not between 0 and 1
