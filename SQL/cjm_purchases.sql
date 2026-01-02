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
(raw_data::jsonb ->> 'purchase_id')::varchar as event_id,
(raw_data::jsonb ->> 'customer_id')::varchar as customer_id,
(raw_data::jsonb ->> 'purchase_dtm')::timestamp as event_dtm,
'order' as event_name,
insert_timestamp as meta_timestamp,
'raw.purchase' as meta_process_name
from raw.purchase
where insert_timestamp > (select coalesce(max(meta_timestamp), '1900-01-01')::timestamp from sergey_r.cjm where meta_process_name = 'raw.purchase')
;
