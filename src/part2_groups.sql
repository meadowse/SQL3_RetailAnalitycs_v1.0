-- в func_group_margin можно передать 4 параметра
-- 	первый - это customer_id
-- 	второй - group_id
-- 	3 - 1 или 2
-- 	    1 - это подсчёт маржи через ограниченное кол-во последних транзакций
-- 	    2 - подсчёт маржи через кол-во дней перед date_of_analysis_formation
-- 	    default - подсчёт маржи через всё кол-во транзакций
-- 	4 - это кол-во дней, либо транзакций (default 0)
drop function if exists func_group_margin;
CREATE OR REPLACE FUNCTION func_group_margin(
        cust_id int,
		gr_id INT,
        pick INT DEFAULT 0,
        amount INT DEFAULT 0)
RETURNS numeric
LANGUAGE plpgsql
AS $$
    DECLARE
    ret numeric;
begin
    if pick = 1 then
with pre as (select group_summ_paid - group_cost as dif from purchases where group_id = gr_id and customer_id = cust_id
order by transaction_datetime desc limit amount)
select round(sum(dif), 6) into ret
from pre;
else if pick = 2 then
with pre as (
select group_summ_paid - group_cost as dif from purchases p
join date_of_analysis_formation d
    on p.transaction_datetime between d.analysis_formation - interval '1 day' * amount and d.analysis_formation
where p.group_id = gr_id and p.customer_id = cust_id
order by p.transaction_datetime desc)
select round(sum(dif), 6) into ret
from pre;
else
    with pre as (select group_summ_paid - group_cost as dif from purchases
                                                            where group_id = gr_id and customer_id = cust_id
                                                            order by transaction_datetime desc)
select round(sum(dif), 6) into ret
from pre;
end if; end if;
return ret;
END; $$;

drop view if exists cgroups;
create or replace view cgroups as (
with churn2 as (select p.customer_id, p.group_id,
       p.group_purchase::double precision /
       (select count(*) from (select * from purchases where customer_id = p.customer_id) as pu
       where transaction_datetime between p.first_group_purchase_date
           and p.last_group_purchase_date)::double precision as group_affinity_index,
       round(EXTRACT(epoch FROM ((select * from date_of_analysis_formation) - p.last_group_purchase_date)/3600/24) / p.group_frequency, 3) as group_churn_rate
from periods p),
inte as (
select ph.customer_id,
	ph.group_id,
        abs(EXTRACT(epoch from(ph.transaction_datetime - lag(ph.transaction_datetime)
            OVER (partition by ph.customer_id, ph.group_id ORDER BY ph.transaction_datetime))/3600/24) - p.group_frequency) / p.group_frequency
            as interim,
            p.group_frequency
        FROM purchases ph join periods p using(customer_id, group_id)),
stab as (select customer_id,
	group_id,
	round(coalesce(avg(interim), 0), 9) as group_stability_index
from inte
group by 1, 2),
discount as (select p.customer_id, p.group_id, d.count::float / p.group_purchase::float as group_discount_share,
       p.group_min_discount as group_minimum_discount
from (select customer_id, group_id, count(sku_discount) from checks join purchases using(transaction_id)
                                                   where sku_discount > 0 group by 1, 2 order by 1, 2) as d
join periods p using(customer_id, group_id)),
averageDiscount as (
select customer_id, group_id, round(sum(group_summ_paid) / sum(group_summ), 7) as group_average_discount
from (select * from purchases where group_summ_paid != group_summ) n group by 1, 2)
select 
	c.customer_id,
	c.group_id,
	c.group_affinity_index,
	c.group_churn_rate,
	s.Group_Stability_Index,

-- 	в func_group_margin можно передать 4 параметра
-- 	первый - это customer_id
-- 	второй - group_id
-- 	3 - 1 или 2
-- 	    1 - это подсчёт маржи через ограниченное кол-во последних транзакций
-- 	    2 - подсчёт маржи через кол-во дней перед date_of_analysis_formation
-- 	    default - подсчёт маржи через всё кол-во транзакций
-- 	4 - это кол-во дней, либо транзакций (default 0)
	func_group_margin(c.customer_id, c.group_id) as group_margin,
	d.Group_Discount_Share,
	d.Group_Minimum_Discount,
	a.Group_Average_Discount
from churn2 c join stab s using(customer_id, group_id)
    join discount d using(customer_id, group_id)
    join averageDiscount a using(customer_id, group_id)
);