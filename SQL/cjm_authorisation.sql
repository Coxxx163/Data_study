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
md5(concat(customer_id, auth_date)) as event_id,
customer_id,
auth_date as event_dtm,
concat('auth ', auth_method) as event_name,
auth_date as meta_timestamp,
'raw.customer_auth' as meta_process_name
from raw.customer_auth
where auth_date > (select coalesce(max(meta_timestamp), '1900-01-01')::timestamp from sergey_r.cjm where meta_process_name = 'raw.customer_auth')
;

