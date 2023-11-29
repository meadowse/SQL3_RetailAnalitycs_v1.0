DROP FUNCTION if EXISTS crosses;
CREATE
OR REPLACE FUNCTION crosses(
    group_amount INT,
    churn_rate NUMERIC,
    stability NUMERIC,
    share_sku NUMERIC,
    margin NUMERIC
) RETURNS TABLE(
    customer_id INT,
    sku_name VARCHAR,
    offer_discount_depth NUMERIC
) LANGUAGE plpgsql AS $$
    BEGIN
        RETURN
            QUERY
            WITH groups AS (SELECT *
            FROM (SELECT *,
                         row_number() over (partition by c.customer_id) as count_limit
            FROM (select * from cgroups c where group_churn_rate <= churn_rate
                                          and group_stability_index <= stability
            order by c.customer_id, group_affinity_index desc) c) c
            WHERE c.count_limit <= group_amount order by c.customer_id,
                                                         group_affinity_index desc),
                 max_margin AS (select cg.customer_id, group_id,
                                       max(sku_retail_price - sku_purchase_price)
                 from cgroups cg join customers c using(customer_id)
                     join stores s on s.transaction_store_id = c.customer_primary_store
                     join product_grid using(group_id, sku_id)
                 group by cg.customer_id, group_id),
                 sku_share as (select *
                 from (select p.customer_id, group_id, sku_id,
                              count(transaction_id) as count_transactions
                 from purchases p join checks c using(transaction_id)
                 group by 1, 2, 3) c join periods p using(customer_id, group_id)
                 where (c.count_transactions::float / group_purchase::float) <= (share_sku * 0.01)),
                 discount as (select *
                 from (select c.customer_id, group_id,
                              round(group_minimum_discount * 100 / 5) * 5 as min_discount,
                              margin * sum(sku_retail_price - sku_purchase_price) / sum(sku_retail_price) as margin_calc
                 from customers c
                     join stores s on c.customer_primary_store = s.transaction_store_id
                     join product_grid using(sku_id)
                     join cgroups cg using(customer_id, group_id) group by 1, 2, 3
                 order by 1, 2) m where m.min_discount <= m.margin_calc
                                    and m.min_discount > 0)
            SELECT g.customer_id, p.sku_name, min_discount as offer_discount_depth
            from groups g
                join customers c using(customer_id)
                join stores s on s.transaction_store_id = c.customer_primary_store
                join product_grid p using(group_id, sku_id)
                join max_margin m using(customer_id, group_id)
                join sku_share sk using(sku_id, customer_id, group_id)
                join discount d using(customer_id, group_id) order by 1;
END $$;

SELECT
    *
FROM
    crosses(5, 7, 3, 100, 90);

SELECT
    *
FROM
    crosses(5, 3, 0.5, 100, 30);

--  1-5.
select *
from (SELECT * FROM (SELECT *, row_number() over (partition by customer_id) as count_limit
                     FROM (select * from cgroups where group_churn_rate <= 3 and group_stability_index <= 0.5
                                                 order by customer_id, group_affinity_index desc) c) c
               WHERE c.count_limit <= 5 order by 1, 3 desc) co
    join customers c using(customer_id)
    join stores s on s.transaction_store_id = c.customer_primary_store
    join product_grid using(group_id, sku_id)
    join
    (select customer_id, group_id, max(sku_retail_price - sku_purchase_price)
    from cgroups
        join customers c using(customer_id)
        join stores s on s.transaction_store_id = c.customer_primary_store
        join product_grid using(group_id, sku_id) group by customer_id, group_id) m using(customer_id, group_id)
    join
    (select * from (select customer_id, group_id, sku_id, count(transaction_id) as count_transactions from purchases
        join checks using(transaction_id) group by 1, 2, 3) c join periods using(customer_id, group_id)
    where (c.count_transactions::float / group_purchase::float) <= (100 * 0.01)) sk using(sku_id, customer_id, group_id)
    join
    (select * from (select customer_id, group_id,
            round(group_minimum_discount * 100 / 5) * 5 as min_discount,
            30 * sum(sku_retail_price - sku_purchase_price) / sum(sku_retail_price) as margin
    from customers c
        join stores s on c.customer_primary_store = s.transaction_store_id
        join product_grid using(sku_id)
        join cgroups using(customer_id, group_id) group by 1, 2, 3 order by 1, 2) m
    where m.min_discount <= m.margin
      and m.min_discount > 0) ma using(customer_id, group_id);

-- 1.
SELECT * FROM (SELECT *, row_number() over (partition by customer_id) as count_limit
                     FROM (select * from cgroups where group_churn_rate <= 3 and group_stability_index <= 0.5
                                                 order by customer_id, group_affinity_index desc) c) c
               WHERE c.count_limit <= 5 order by 1, 3 desc;

-- 2.
select customer_id, group_id, max(sku_retail_price - sku_purchase_price)
    from cgroups
        join customers c using(customer_id)
        join stores s on s.transaction_store_id = c.customer_primary_store
        join product_grid using(group_id, sku_id) group by customer_id, group_id;

-- 3.
select * from (select customer_id, group_id, sku_id, count(transaction_id) as count_transactions from purchases
        join checks using(transaction_id) group by 1, 2, 3) c join periods using(customer_id, group_id)
    where (c.count_transactions::float / group_purchase::float) <= (100 * 0.01);

-- 4-5.
select * from (select customer_id, group_id,
            round(group_minimum_discount * 100 / 5) * 5 as min_discount,
            30 * sum(sku_retail_price - sku_purchase_price) / sum(sku_retail_price) as margin
    from customers c
        join stores s on c.customer_primary_store = s.transaction_store_id
        join product_grid using(sku_id)
        join cgroups using(customer_id, group_id) group by 1, 2, 3 order by 1, 2) m
    where m.min_discount <= m.margin
      and m.min_discount > 0