--СБОРКА ЕЖЕДНЕВНОГО ИНКРЕМЕНТА 
create temp table orders_date_increment as 
select distinct order_date::date
from core.orders
where order_date::date = now()::date - 1
;

create temp table orders_increment as

with sorted_orders as
(select
date_trunc('day', o.order_date)::date as order_date,
case
	when o.channel ilike '%partner%' then 'партнеры'
	when o.channel ilike '%organic%' then 'органика'
	when o.channel = 'refferal' then 'реферал'
end as channel_name,
case
	when o.order_type = 'paid' then 'оплачен'
	when o.order_type = 'search' then 'не оплачен'
	else 'не определено'
end as order_type_name,
case
	when o.interface = 'web' then 'веб'
	when o.interface = 'app' then 'приложение'
	else 'не определено'
end as interface_name,
count(o.id) as orders_count,
count(distinct o.customer_id) as customers_count,
sum(quantity) as quantity_amount,
sum(amount::numeric * quantity::numeric * (100 - coalesce(discount::numeric, 0))/100) as revenue
from core.orders o
where date_trunc('day', order_date)::date in (select order_date from orders_date_increment)
group by 1, 2, 3, 4
),

crm as
(
select
date_trunc('day', dtm)::date as crm_date,
sum(cost) as "cost"
from sergey_r.crm_mart
where date_trunc('day', dtm)::date in (select order_date from orders_date_increment)
group by 1
)
,

orders_by_day as
(
select so.*,
sum(orders_count) over (partition by so.order_date) as orders_count_day,
sum(customers_count) over (partition by so.order_date) as customers_count_day,
sum(quantity_amount) over (partition by so.order_date) as quantity_amount_day,
sum(revenue) over (partition by so.order_date) as revenue_day,
coalesce(c.cost, 0) as crm_cost
from sorted_orders so
left join crm c
on so.order_date = c.crm_date
)

select
order_date,
channel_name,
order_type_name,
interface_name,
orders_count,
customers_count,
quantity_amount,
revenue,
orders_count_day,
customers_count_day,
quantity_amount_day,
revenue_day,
orders_count / orders_count_day *100 as orders_percent,
customers_count / customers_count_day * 100 as customers_percent,
quantity_amount / quantity_amount_day * 100 as quantity_percent,
revenue / revenue_day * 100 as revenue_percent,
crm_cost
from orders_by_day;

delete from sergey_r.fin_metrics fm
using orders_date_increment inc
where fm.order_date = inc.order_date;

INSERT INTO sergey_r.fin_metrics
SELECT *
FROM orders_increment;
