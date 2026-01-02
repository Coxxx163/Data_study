CREATE VIEW sergey_r.rfm_mart AS
SELECT customer_id, 
count(distinct id) AS orders_count, -- общее кол-во заказов клиента
count(distinct id) FILTER (WHERE o.order_type  = 'paid') AS orders_paid_count, -- общее кол-во оплаченных заказов клиента
date_part('day', current_date - max(order_date)) AS days_last_order, -- время в днях с момента последнего оплаченного заказа клиентом до текущего момента
min(order_date) FILTER (WHERE order_type = 'paid')::date AS first_paid_order_date, -- дата первого оплаченного заказа
sum(amount * quantity * ((100 - coalesce(discount, 0)) / 100)) AS orders_revenue, -- общая выручка всех заказов клиента
sum(amount * quantity * ((100 - coalesce(discount, 0)) / 100)) FILTER (WHERE order_type = 'paid') AS orders_paid_revenue, -- общая выручка оплаченных заказов клиента
sum(amount * quantity * ((100 - coalesce(discount, 0)) / 100)) filter (where order_type = 'paid') / NULLIF(count(id) FILTER (WHERE order_type = 'paid'), 0) AS avg_bill -- средний чек всех оплаченных заказов клиента
FROM core.orders o 
GROUP BY 1;

select *
from sergey_r.rfm_mart
