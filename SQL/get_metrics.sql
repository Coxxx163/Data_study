create or replace function sergey_r.get_metrics(
    start_date date,
    end_date date
)
returns table (
    total_visits bigint,
    total_users bigint,
    phone_users numeric,
    web_users numeric,
    foreign_users numeric,
    target_users bigint,
    target_users_part numeric,
    retention_target_users numeric
)
as $$
begin
    return QUERY
		with goal_events as (
		select
		counteruseridhash,
		min(datetime) as goal_datetime
		from sergey_r.ym_mart
		where page_title = 'ОАД - Способы оплаты'
		group by 1
		)
    	select
		count(distinct m.visitid) as total_visits,
    	count(distinct m.counteruseridhash) as total_users,
    	round(count(distinct m.counteruseridhash) filter (where device_type = 'phone')::numeric / count(distinct m.counteruseridhash)::numeric *100, 2) as phone_users,
		round(count(distinct m.counteruseridhash) filter (where device_type = 'laptop')::numeric / count(distinct m.counteruseridhash)::numeric *100, 2) as web_users,
		round(count(distinct m.counteruseridhash) filter (where regioncountry not like 'Russia')::numeric / count(distinct m.counteruseridhash)::numeric *100, 2) as foreign_users,
		count(distinct m.counteruseridhash) filter (where page_title = 'ОАД - Способы оплаты') as target_users,
		round(count(distinct m.counteruseridhash) filter (where page_title = 'ОАД - Способы оплаты')::numeric / count(distinct m.counteruseridhash)::numeric *100, 2) as target_users_part,
		round(count(distinct g.counteruseridhash) filter (where event_type in ('view', 'click'))::numeric / count(distinct m.counteruseridhash) filter (where page_title = 'ОАД - Способы оплаты')::numeric *100, 2) as retention_target_users
    	from
        sergey_r.ym_mart m
		left join goal_events g
		on m.counteruseridhash = g.counteruseridhash
		and m.datetime between g.goal_datetime and (g.goal_datetime + interval '1 month')
		where
        datetime::date between start_date and end_date;
end;
$$ language plpgsql;

select * from sergey_r.get_metrics('2025-07-01', '2025-09-01')
