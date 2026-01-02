create table if not exists sergey_r.cjm (
	event_id varchar null,
	customer_id varchar null,
	event_dtm date null,
	event_name varchar null,
	meta_timestamp timestamp null,
	meta_process_name varchar null
);
insert into sergey_r.cjm
select distinct on (c.customer_email, event_dtm)
md5(concat(c.customer_email, (raw_data::jsonb ->> 'dtm')::timestamp)) as event_id,
customer_id,
(raw_data::jsonb ->> 'dtm')::timestamp as event_dtm,
'email' as event_name,
s.insert_timestamp as meta_timestamp,
'raw.crm_email' as meta_process_name
from raw.crm_email s
join core.customers c 
on (raw_data::jsonb ->> 'email')::varchar = c.customer_email
and (raw_data::jsonb ->> 'dtm')::timestamp > registration_dtm
where insert_timestamp > (select coalesce(max(meta_timestamp), '1900-01-01')::timestamp from sergey_r.cjm where meta_process_name = 'raw.crm_email')
order by c.customer_email, (raw_data::jsonb ->> 'dtm')::timestamp, registration_dtm desc
