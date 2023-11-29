drop function if exists Check_Measure;
CREATE OR REPLACE FUNCTION Check_Measure(
    cust_id int,
	variant int, 
    start_date timestamp,
    end_date timestamp,
    trans_count int, 
    rate_check_measure numeric)
returns float
LANGUAGE plpgsql
AS $$
    BEGIN
	if variant = 1 then
	    -- by period
	    RETURN (select round((sum(group_summ) / count(group_summ)) * rate_check_measure, 2) from purchases where customer_id = cust_id
	                                                                                           and transaction_datetime
	                                                                                               between start_date and end_date);
	else
		-- by transactions
	    RETURN (select round((sum(group_summ) / count(group_summ)) * rate_check_measure, 2) from purchases where customer_id = cust_id
	    limit trans_count);
	end if;	
END; $$;

-- select * from Check_Measure(1, 1, '2020-01-01', '2022-01-01', 5, 1.5);
-- select Check_Measure from Check_Measure(1, 2, '2020-01-01', '2021-01-01', 5, 1.5);

drop function if exists average_check_growth;
CREATE OR REPLACE FUNCTION average_check_growth(
    variant int, 
    start_date timestamp,
    end_date timestamp,
    trans_count int, 
    rate_check_measure numeric, 
    max_churn_rate numeric, 
    max_discount_share numeric, 
    margin numeric) 
RETURNS table(customer_id INT,
        required_check_measure numeric, 
        group_name varchar, 
        offer_discount_depth int) 
LANGUAGE plpgsql
AS $$
BEGIN
RETURN QUERY
    select m.customer_id,
           Check_Measure(m.customer_id, variant, start_date, end_date, trans_count, rate_check_measure)::numeric as required_check_measure,
           s.group_name,
           (round(c.group_minimum_discount * 100 / 5) * 5)::int as offer_discount_depth
    from (select w.customer_id, max(w.group_affinity_index) as group_affinity_index
          from (select *
          from (select c.customer_id, group_id,
                       margin * sum(sku_retail_price - sku_purchase_price) / sum(sku_retail_price) as group_margin
          from cards c join transactions using(customer_card_id)
              join checks using(transaction_id)
              join product_grid using(sku_id)
              join stores using(transaction_store_id, sku_id) group by 1, 2
          order by 1, 2) m join cgroups c using(customer_id, group_id)
          where c.group_churn_rate <= max_churn_rate
            and c.group_discount_share <= max_discount_share * 0.01
            and round(c.group_minimum_discount * 100 / 5) * 5 > 0
            and round(c.group_minimum_discount * 100 / 5) * 5 <= m.group_margin
          order by 1, 2) w group by 1) m
        join cgroups c using(customer_id, group_affinity_index)
        join sku_group s using(group_id)
    order by offer_discount_depth, required_check_measure;
END; $$;

select * from average_check_growth(1, '2020-01-01', '2022-01-01', 2, 1.5, 7, 3, 60);
select * from average_check_growth(2, '2020-01-01', '2022-01-01', 2, 1.5, 7, 3, 60);

select * from average_check_growth(2, '2020-01-01', '2022-01-01', 100, 1.15, 3, 70, 30);

select customer_id, group_id,
       round(group_minimum_discount * 100 / 5) * 5 as min_discount,
       m.margin
from (select customer_id, group_id,
             30 * sum(group_summ - group_cost) / sum(group_summ) as margin
from purchases group by 1, 2) m
    join cgroups using(customer_id, group_id)
where round(group_minimum_discount * 100 / 5) * 5 > 0
  and round(group_minimum_discount * 100 / 5) * 5 <= m.margin order by 1, 2;

select customer_id, group_id, round(group_minimum_discount * 100 / 5) * 5,
       m.margin from (select customer_id, group_id,
       30 * sum(sku_retail_price - sku_purchase_price) / sum(sku_retail_price) as margin from cards join transactions using(customer_card_id)
    join checks using(transaction_id)
    join product_grid using(sku_id)
    join stores using(transaction_store_id, sku_id) group by 1, 2 order by 1, 2) m
    join cgroups using(customer_id, group_id)
where round(group_minimum_discount * 100 / 5) * 5 > 0
  and round(group_minimum_discount * 100 / 5) * 5 <= m.margin order by 1, 2