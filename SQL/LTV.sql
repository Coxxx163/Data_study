--ИНКРЕМЕНТ

--расчет ltv
create temp table ltv as
select 
crm.customer_id,
round(sum(amount::numeric * quantity::numeric * (100 - coalesce(discount::numeric, 0))/100), 4) - sum(cost) as ltv,
now()::timestamp as calculate_dtm
from sergey_r.crm_mart crm
left join core.orders o
using (customer_id)
where order_type = 'paid' and status not in ('not sent', 'error')
group by 1;

--закрытие старых значений ltv (перевод в false)
update sergey_r.ltv ltv
set is_actual = false
from ltv l
where ltv.customer_id = l.customer_id
and ltv.is_actual = true
and ltv.ltv is distinct from l.ltv;

--вставка актуальных ltv
insert into sergey_r.ltv
select l.*,
true as is_actual
from ltv l
left join sergey_r.ltv ltv
on l.customer_id = ltv.customer_id
and ltv.is_actual = true
where ltv.customer_id is null
or ltv.ltv is distinct from l.ltv;

--создание snapshot
insert into sergey_r.ltv_snapshots
select
customer_id,
ltv,
calculate_dtm
from ltv
