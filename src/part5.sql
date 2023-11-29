SET
    datestyle = 'ISO,DMY';

drop function if exists func_datetime;
create or replace function func_datetime(
cust_id int,
start_date timestamp,
end_date timestamp)
returns int
language plpgsql
as $$
    DECLARE
    res int;
    begin
    if (select count(*) from purchases where transaction_datetime between start_date and end_date and customer_id = cust_id) = 0 then res = 0;
    else res = round(EXTRACT(epoch FROM(end_date - start_date)) /3600/24 / (select count(*) from purchases where transaction_datetime between start_date and end_date and customer_id = cust_id))::int;
        end if;
    return res;
    end $$;

drop function if exists do_it_faster;
CREATE OR REPLACE FUNCTION do_it_faster(
    start_dat timestamp,
    end_dat timestamp,
    transactions_amount int,
    max_churn_rate numeric,
    max_discount_share numeric,
    margin numeric)
RETURNS table
    (customer_id int, 
    start_date timestamp,
    end_date timestamp,
    required_transactions_count int,
    group_name varchar, 
    offer_discount_depth int)
LANGUAGE plpgsql
AS $$
    BEGIN
RETURN QUERY
	select m.customer_id, start_dat, end_dat,
	       func_datetime(m.customer_id, start_dat, end_dat) + transactions_amount as Required_Transactions_Count,
           s.group_name, (round(c.group_minimum_discount * 20) * 5)::int as offer_discount_depth
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
    order by offer_discount_depth;
END $$;

SELECT * FROM do_it_faster('2020-01-01', '2022-01-01', 2, 7, 70, 60);

select * from do_it_faster('18.08.2022 00:00:00', '18.08.2022 00:00:00', 1, 3, 70, 30);