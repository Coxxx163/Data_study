create table if not exists sergey_r.ym_mart (
	watchid varchar null,
	counteruseridhash varchar null,
	datetime timestamp null,
	referer varchar null,
	url varchar null,
	external_resource varchar null,
	page_title varchar null,
	device_type varchar null,
	event_type varchar null,
	meta_timestamp timestamp null,
	visitid varchar null,
	isnewuser boolean null,
	starturl varchar null,
	endurl varchar null,
	pageviews integer null,
	visitduration integer null,
	regioncountry varchar null,
	regioncity varchar null
);

insert into sergey_r.ym_mart
with watchids as (
select *,
unnest(string_to_array(trim(watchids, '[]'), ',')) as watchid
from raw.ym_visits)

select
yh.watchid,
yh.counteruseridhash,
yh.datetime:: timestamp,
referer,
split_part(url, '?', 1) as url,
case
	when url in ('%t.me%', '%tgc%') then 'tg'
	when url like '%google%forms%' then 'google_forms'
	when url like '%github%' then 'github'
	when url like '%disk.yandex%' then 'yandex_disk'
	when url like '%vk.com%' then 'vk_group'
	when url like '%instagram%' then 'instagram'
	when url like '%youtube%' then 'youtube'
	when url like '%drive.google%' then 'google_drive'
end as external_resource,
case
	when url = 'https://datastudy.ru/1' then 'ОАД'
	when url = 'https://datastudy.ru/1#rec483887514' then 'ОАД - Программа курса'
	when url = 'https://datastudy.ru/1#rec631510136' then 'ОАД - Способы оплаты'
	when url = 'https://datastudy.ru/1#rec850109589' then 'ОАД - Отзывы'
	when url = 'https://datastudy.ru/1#rec486716212' then 'FAQ'
	when url = 'https://datastudy.ru/oferta' then 'Оферта'
	when url = 'https://datastudy.ru/daniildzheparov' then 'Автор курсов'
	when url = 'https://datastudy.ru/#rec850091618' then 'Курсы'
	when url = 'https://datastudy.ru/cases' then 'Кейсы'
	when url = 'https://datastudy.ru/#rec852933749' then 'Отзывы'
	when url = 'https://datastudy.ru/spasibo' then 'Спасибо'
	when url = 'https://datastudy.ru/zerolesson' then 'Урок 0'
	when url = 'https://datastudy.ru/firstlessonfirst' then 'Урок 1.1'
	when url = 'https://datastudy.ru/firstlessonsecond' then 'Урок 1.2'
	when url = 'https://datastudy.ru/secondlessonfirst' then 'Урок 2.1'
	when url = 'https://datastudy.ru/secondlessonsecond' then 'Урок 2.2'
	when url = 'https://datastudy.ru/' then 'Главная'
	else split_part(url, 'ru/', 2)
end as page_title,
case
	when devicecategory = '1' then 'laptop'
	when devicecategory = '2' then 'phone'
end as device_type,
case
	when ispageview > link and ispageview > artificial then 'view'
	when link > ispageview and link > artificial then 'click'
	when artificial > ispageview and artificial > link then 'artificial'
	when ispageview = '1' and link = '1' then 'view'
	when ispageview = '1' and artificial = '1' then 'view'
	when link = '1' and artificial = '1' then 'click'
end as event_type,
yh.insert_dtm as meta_timestamp,
visitid,
isnewuser::boolean,
starturl,
endurl,
pageviews::integer,
visitduration::integer,
regioncountry,
regioncity
from raw.ym_hits yh
left join watchids w 
on yh.watchid = w.watchid 
where yh.insert_dtm > (select coalesce(max(meta_timestamp), '1900-01-01')::timestamp from sergey_r.ym_mart ym )
