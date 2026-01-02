create table if not exists sergey_r.customers_base_mart
(
	base_date date,
	authorized_customers int8,
	deleted_customers int8,
	authorized_customers_cum int8,
	deleted_customers_cum int8,
	active_customers int8,
	active_base_delta int8
);

create table if not exists sergey_r.customers_base_mart_actual AS

with first_auth as (
select distinct on (customer_id)
customer_id,
auth_date::date as first_auth
from raw.customer_auth
WHERE customer_id IS NOT null
order by 1, 2
),

deleted as (
select
customer_id,
customer_delete_dtm::date as delete_date
from raw.customer_delete
),

daily_auth as (
select
first_auth as base_date,
count(customer_id) as authorized_customers
from first_auth
group by 1
),

daily_del as (
select
delete_date as base_date,
count(customer_id) as deleted_customers
from deleted
group by 1
),

auth_del as (
select
coalesce(a.base_date, d.base_date) as base_date,
coalesce(a.authorized_customers, 0) as authorized_customers,
coalesce(d.deleted_customers, 0) as deleted_customers,
sum(authorized_customers) over (order by base_date rows between unbounded preceding and current row) as authorized_customers_cum,
sum(deleted_customers) over (order by base_date rows between unbounded preceding and current row) as deleted_customers_cum
from daily_auth a
full outer join daily_del d
using (base_date)
order by 1),

active as (
select 
*,
authorized_customers_cum - deleted_customers_cum as active_customers
from auth_del)

select
*,
active_customers - lag(active_customers) over (order by base_date) as active_base_delta,
current_timestamp AS meta_timestamp
from active;

--переименуем исходную таблицу с префиксом _old
alter table sergey_r.customers_base_mart
rename to customers_base_mart_old
;

--переименуем таблицу с актуальными данными в целевое названием
alter table sergey_r.customers_base_mart_actual
rename to customers_base_mart
;

--удаляем таблицу со старой версией данных
drop table sergey_r.customers_base_mart_old
;
