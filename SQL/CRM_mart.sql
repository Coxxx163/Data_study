--СБОРКА ИНКРЕМЕНТА
insert into sergey_r.crm_mart
with last_dtm as (
    select coalesce(max(dtm), '1900-01-01'::timestamp) as max_dtm
    from sergey_r.crm_mart
),

crm as
(select
concat(id::varchar, '_s') as com_id,
case
	when (raw_data::jsonb)::varchar like '%phone%' then 'sms'
end as com_type,
(raw_data::jsonb ->> 'dtm')::timestamp as dtm,
(raw_data::jsonb ->> 'phone')::varchar as contact,
(raw_data::jsonb ->> 'status')::varchar as status,
(raw_data::jsonb ->> 'provider')::varchar as provider,
(raw_data::jsonb ->> 'type')::varchar as "type",
(raw_data::jsonb ->> 'rate')::varchar as rate
from raw.crm_sms
union all
select 
concat(id::varchar, '_e') as com_id,
case
	when (raw_data::jsonb)::varchar like '%email%' then 'email'
end as com_type,
(raw_data::jsonb ->> 'dtm')::timestamp as dtm,
(raw_data::jsonb ->> 'email')::varchar as contact,
(raw_data::jsonb ->> 'status')::varchar as status,
(raw_data::jsonb ->> 'provider')::varchar as provider,
(raw_data::jsonb ->> 'type')::varchar as "type",
null as rate
from raw.crm_email),

customers_sms as
(select
crm.*,
c.customer_id
from crm crm
left join core.customers c
on crm.contact = c.customer_phone
where com_type = 'sms'),

customers_email as
(select
crm.*,
c.customer_id
from crm crm
left join core.customers c
on crm.contact = c.customer_email
where com_type = 'email'),

customers_crm as
(select *
from customers_sms
union all
select *
from customers_email)

select
cc.com_id,
cc.com_type,
cc.dtm,
cc.contact,
cc.status,
cc.provider,
cc.type,
cc.rate,
cc.customer_id,
gs.price as "cost",
now() as meta_timestamp
from customers_crm cc
left join sergey_r.google_sheets_data gs
on cc.rate = gs.tariff
and
concat_ws('-', date_part('year', cc.dtm::date), date_part('quarter', cc.dtm::date)) = concat_ws('-', date_part('year', gs.sheet_date::date), date_part('quarter', gs.sheet_date::date))
and 
cc.provider = gs.provider
and
cc.type = gs.send_type
where cc.dtm > (select max_dtm from last_dtm)
