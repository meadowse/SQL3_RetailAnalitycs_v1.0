DROP view if EXISTS purchases;
CREATE
OR replace view purchases AS (
    SELECT
        c.customer_id,
        t.transaction_id,
        t.transaction_datetime,
        pg.group_id,
        round(ch.sku_amount * s.sku_purchase_price, 8) AS group_cost,
        ch.sku_summ AS group_summ,
        ch.sku_summ_paid AS group_summ_paid
    FROM
        cards c
        JOIN transactions t USING(customer_card_id)
        JOIN checks ch USING(transaction_id)
        JOIN product_grid pg USING(sku_id)
        JOIN stores s USING(transaction_store_id, sku_id)
    ORDER BY
        1,
        4,
        3 desc
);
-- 1-6.
SELECT
    customer_id,
    transaction_id,
    transaction_datetime,
    group_id
FROM
    cards
    JOIN transactions USING(customer_card_id)
    JOIN checks USING(transaction_id)
    JOIN product_grid USING(sku_id)
ORDER BY
    1,
    4,
    3 desc;
-- 7.
SELECT
    customer_id,
    transaction_id,
    transaction_datetime,
    group_id,
    sku_amount,
    sku_purchase_price,
    sku_summ,
    sku_summ_paid
FROM
    cards
    JOIN transactions USING(customer_card_id)
    JOIN checks USING(transaction_id)
    JOIN product_grid USING(sku_id)
    JOIN stores USING(transaction_store_id, sku_id)
ORDER BY
    1,
    4,
    3 desc;
SELECT
    *
FROM
    purchases
LIMIT
    2 offset 71;
SELECT
    p.customer_id,
    SUM(p.group_summ_paid - p.group_cost)
FROM
    purchases p
GROUP BY
    1
HAVING
    SUM(p.group_summ_paid - p.group_cost) BETWEEN 0 AND 1000;