-- Silver ya pone en cuarentena estas devoluciones; si alguna llega a gold es un bug.
select * from {{ ref('fct_refunds') }}
where payment_amount is not null and amount > payment_amount
