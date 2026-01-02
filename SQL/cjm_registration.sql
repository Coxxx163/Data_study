create table if not exists sergey_r.cjm (
	event_id varchar null,
	customer_id varchar null,
	event_dtm date null,
	event_name varchar null,
	meta_timestamp timestamp null,
	meta_process_name varchar null
);
insert into sergey_r.cjm
select
md5(concat((raw_data::jsonb ->> 'uid')::varchar, (raw_data::jsonb -> 'registration_details' ->> 'registration_dtm')::varchar)) as event_id,
(raw_data::jsonb ->> 'uid')::varchar as customer_id,
(raw_data::jsonb -> 'registration_details' ->> 'registration_dtm')::timestamp as event_dtm,
'registration' as event_name,
insert_timestamp as meta_timestamp,
'raw.customer' as meta_process_name
from raw.customer
where insert_timestamp > (select coalesce(max(meta_timestamp), '1900-01-01')::timestamp from sergey_r.cjm where meta_process_name = 'raw.customer')
;
