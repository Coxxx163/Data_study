drop table if exists sergey_r.retention_mart;

create table if not exists sergey_r.retention_mart 
(
	cohort_month date null,
	cohort_size int null,
	order_month date null,
	month_offset int8 null,
	customers int8 null,
	retention_rate float8 null,
	meta_timestamp timestamp default current_timestamp
);

truncate table sergey_r.retention_mart
;

insert into sergey_r.retention_mart
with cohort_month as (
select 
customer_id,
date_trunc('month', min(order_date))::date as cohort_month
from core.orders
where order_type = 'paid'
group by 1
),

cohort_size as (
select customer_id,
count(customer_id) over (partition by cohort_month) as cohort_size
from cohort_month
),

order_month as (
select
customer_id,
date_trunc('month', order_date)::date as order_month
from core.orders
where order_type = 'paid'
group by 1, 2
),

customers as (
select
cm.cohort_month,
om.order_month,
count(distinct om.customer_id) as customers
from order_month as om
join cohort_month as cm
on om.customer_id = cm.customer_id
group by 1, 2
)

select
cm.cohort_month,
cohort_size,
om.order_month,
extract (month from age(om.order_month, cm.cohort_month)) as month_offset,
customers,
round(customers::numeric/cohort_size::numeric*100, 2) as retention_rate
from cohort_month as cm
join order_month as om
using (customer_id)
join customers as cu
using (cohort_month, order_month)
join cohort_size as cs
using (customer_id)
group by 1, 2, 3, 5
order by 1, 3
