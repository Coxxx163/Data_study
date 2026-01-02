-- СБОРКА ИНКРЕМЕНТА-----------------------------------------------------------------------------------

-- таблица с customer_id тех, кто зарегистрировался и авторизовался после фиксации в исходной витрине
create temp table customer_id_increment AS 
select customer_id
from raw.customer_auth
where auth_date > (select COALESCE(max(last_auth_dtm), '1900-01-01')::timestamp from sergey_r.customer_mart)
union all 
select (raw_data::jsonb ->> 'uid')::varchar
from raw.customer
where (raw_data::jsonb -> 'registration_details' ->> 'registration_dtm')::timestamp > (select COALESCE(max(registration_dtm), '1900-01-01')::timestamp from sergey_r.customer_mart)
union all
select customer_id
from raw.customer_delete
where customer_delete_dtm > (select COALESCE(max(customer_delete_dtm), '1900-01-01')::timestamp from sergey_r.customer_mart)
;

-- собираем данные инкремента только для отобранных customer_id
create temp table customer_increment as
with customer as --основная таблица с данными о клиентах
(select
(raw_data::jsonb ->> 'uid')::varchar as customer_id,
split_part((raw_data::jsonb ->> 'full_name')::varchar, ' ', 1) as first_name,
split_part((raw_data::jsonb ->> 'full_name')::varchar, ' ', 2) as last_name,
(raw_data::jsonb ->> 'birth_dtm')::date as birth_date,
case
	when (raw_data::jsonb ->> 'gender')::varchar = 'male' then 'M'
	when (raw_data::jsonb ->> 'gender')::varchar = 'female' then 'F'
end as gender,
(raw_data::jsonb ->> 'phone')::varchar as customer_phone,
(raw_data::jsonb ->> 'email')::varchar as customer_email,
(raw_data::jsonb -> 'registration_details' ->> 'registration_dtm')::timestamp as registration_dtm,
(raw_data::jsonb -> 'registration_details' ->> 'regustration_source')::varchar as registration_source
from raw.customer с)
,
auth_phone as-- таблица с данными об авторизациях по телефону
(
select
customer_id,
case
	when auth_method in ('sms', 'push', '2fa', 'mfa') then true
end as phone_is_confirmed	
from raw.customer_auth
where auth_method in ('sms', 'push', '2fa', 'mfa')
group by 1, 2
)
,
auth_email as -- таблица с данными об авторизациях по email
(
select
customer_id,
case
	when auth_method = 'email' then true
end as email_is_confirmed	
from raw.customer_auth
where auth_method = 'email'
group by 1, 2)
,
first_auth as -- таблица с данными о первой авторизации
(
select
distinct on (customer_id)
customer_id,
auth_method as first_auth_method,
auth_date as first_auth_dtm
from raw.customer_auth)
,
last_auth as -- таблица с данными о краней авторизации
(
select
distinct on (customer_id)
customer_id,
auth_method as last_auth_method,
auth_date as last_auth_dtm
from raw.customer_auth
order by 1, 3 desc)
,
auth_methods as --таблица с данными о методах авторизации
(
select
customer_id,
string_agg(distinct auth_method, ', ') as auth_methods,
count(*) as auth_count
from raw.customer_auth
where auth_method not like ''
group by 1)
,
actual_passport as -- таблица с актуальными паспортными данными
(
select
distinct on ((raw_data::jsonb ->> 'customer_id')::varchar)
(raw_data::jsonb ->> 'customer_id')::varchar as customer_id,
(raw_data::jsonb ->> 'document_type')::varchar,
(raw_data::jsonb ->> 'series')::varchar as passport_series,
(raw_data::jsonb ->> 'number')::varchar as passport_number,
(raw_data::jsonb ->> 'valid_dtm')::varchar as passport_valid_dtm,
insert_timestamp
from raw.customer_documents
where (raw_data::jsonb ->> 'document_type')::varchar like '%passport'
group by 1, 2, 3, 4, 5, 6
order by 1, 6 desc)
,
actual_dl as -- таблица с актуальными данными о правах
(
select
distinct on ((raw_data::jsonb ->> 'customer_id')::varchar)
(raw_data::jsonb ->> 'customer_id')::varchar as customer_id,
(raw_data::jsonb ->> 'document_type')::varchar,
(raw_data::jsonb ->> 'series')::varchar as dl_series,
(raw_data::jsonb ->> 'number')::varchar as dl_number,
(raw_data::jsonb ->> 'valid_dtm')::varchar as dl_valid_dtm,
insert_timestamp
from raw.customer_documents
where (raw_data::jsonb ->> 'document_type')::varchar like 'driver%'
group by 1, 2, 3, 4, 5, 6
order by 1, 6 desc)
,
all_passports as -- таблица с данными о паспортах
(
select
(raw_data::jsonb ->> 'customer_id')::varchar as customer_id,
to_jsonb(string_agg((concat_ws(' ', (raw_data::jsonb ->> 'series')::varchar, (raw_data::jsonb ->> 'number')::varchar)), ', ')) as passport_all
from raw.customer_documents
where (raw_data::jsonb ->> 'document_type')::varchar like '%passport'
group by 1)
,
all_dl as -- таблица с данными о всех правах
(
select
(raw_data::jsonb ->> 'customer_id')::varchar as customer_id,
to_jsonb(string_agg((concat_ws(' ', (raw_data::jsonb ->> 'series')::varchar, (raw_data::jsonb ->> 'number')::varchar)), ', ')) as dl_all
from raw.customer_documents
where (raw_data::jsonb ->> 'document_type')::varchar like 'driver%'
group by 1)
-- вставка данных в витрину
select
c.customer_id,
c.first_name,
c.last_name,
c.birth_date,
c.gender,
c.customer_phone,
c.customer_email,
ap.phone_is_confirmed,
ae.email_is_confirmed,
c.registration_dtm,
c.registration_source,
fa.first_auth_method,
fa.first_auth_dtm,
la.last_auth_method,
la.last_auth_dtm,
am.auth_methods,
am.auth_count,
pass.passport_series,
pass.passport_number,
pass.passport_valid_dtm,
dl.dl_series,
dl.dl_number,
dl.dl_valid_dtm,
passall.passport_all,
dlall.dl_all,
del.customer_delete_dtm
from customer c
left join auth_phone ap
using (customer_id)
left join auth_email ae
using (customer_id)
left join first_auth fa
using (customer_id)
left join last_auth la
using (customer_id)
left join auth_methods am
using (customer_id)
left join actual_passport pass
using (customer_id)
left join actual_dl dl
using (customer_id)
left join all_passports passall
using (customer_id)
left join all_dl dlall
using (customer_id)
left join raw.customer_delete del
using (customer_id)
where exists (select 1 from customer_id_increment inc where c.customer_id = inc.customer_id)
;

--удаляем данные из витрины по клиентам, которые есть в инкременте, чтобы обновить их
delete from sergey_r.customer_mart
where exists (select 1 from customer_increment inc where inc.customer_id = sergey_r.customer_mart.customer_id);

--вставляем актуальные данные инкремента в витрину
insert into sergey_r.customer_mart
select *
from customer_increment
