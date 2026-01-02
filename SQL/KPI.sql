--СБОРКА ИНКРЕМЕНТА
delete from sergey_r.kpi
where order_day >= now()::date - 1;

insert into sergey_r.kpi

with revenue_day as
(select
date_trunc('month', order_date)::date as order_month,
date_trunc('day', order_date)::date as order_day,
sum(amount::numeric * quantity::numeric * (100 - coalesce(discount::numeric, 0))/100) as fact_revenue_day
from core.orders
where order_type = 'paid'
group by 1, 2
order by 1, 2),

cum_revenue as
(
select d.*,
sum(fact_revenue_day) over (partition by order_month order by order_day) as cum_revenue_in_month
from revenue_day d
),

month_revenue as
(
select
distinct on (order_month)
order_month,
cum_revenue_in_month as month_revenue
from cum_revenue
order by 1, 2 desc
),

plan_revenue as
(
select
order_month,
lag(month_revenue) over (order by order_month) as plan_minimum_revenue
from month_revenue
)

select
c.*,
p.plan_minimum_revenue,
cum_revenue_in_month::numeric / plan_minimum_revenue::numeric * 100 as plan_reach_percent,
case
	when cum_revenue_in_month::numeric / plan_minimum_revenue::numeric * 100 < 100
	or cum_revenue_in_month::numeric / plan_minimum_revenue::numeric * 100 is null then false
	else true
end as plan_is_reached
from cum_revenue c
left join plan_revenue p
using (order_month)
where order_day = now()::date - 1
