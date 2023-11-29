DROP FUNCTION if EXISTS func_group_min_discount;
CREATE
OR REPLACE FUNCTION func_group_min_discount(cust_id INT, gr_id INT) RETURNS NUMERIC LANGUAGE plpgsql AS $$
    DECLARE ret NUMERIC;
BEGIN WITH pre AS (
    SELECT
        sku_discount
    FROM
        purchases
        JOIN checks USING(transaction_id)
    WHERE
        group_id = gr_id
        AND customer_id = cust_id
)
SELECT
    SUM(sku_discount) INTO ret
FROM
    pre;
if ret != 0 THEN
SELECT
    MIN(sku_discount / sku_summ) INTO ret
FROM
    checks
    JOIN purchases USING(transaction_id)
WHERE
    sku_discount > 0
    AND group_id = gr_id
    AND customer_id = cust_id;
END if;
RETURN ret;
END;
$$;
DROP view if EXISTS periods;
CREATE
OR replace view periods AS (
    SELECT
        p.customer_id,
        p.group_id,
        p.first_group_purchase_date,
        p.last_group_purchase_date,
        p.group_purchase,
        p.group_frequency,
        round(
            func_group_min_discount(p.customer_id, p.group_id),
            9
        ) AS group_min_discount
    FROM
        (
            SELECT
                p.customer_id,
                p.group_id,
                MIN(p.transaction_datetime) AS first_group_purchase_date,
                MAX(p.transaction_datetime) AS last_group_purchase_date,
                COUNT(p.transaction_id) AS group_purchase,
                (
                    EXTRACT(
                        epoch
                        FROM
                            (
                                MAX(p.transaction_datetime) - MIN(p.transaction_datetime)
                            ) / 3600 / 24
                    ) + 1
                ) / COUNT(p.transaction_id) AS group_frequency
            FROM
                purchases p
                JOIN checks c USING(transaction_id)
            GROUP BY
                1,
                2
        ) p
);
SELECT
    customer_id,
    transaction_id,
    transaction_datetime,
    group_id,
    sku_discount,
    sku_summ
FROM
    purchases
    JOIN checks USING(transaction_id)
ORDER BY
    1,
    4,
    3 desc;
SELECT
    *
FROM
    periods
WHERE
    Group_Min_Discount > 0.3;
SELECT
    *
FROM
    periods
ORDER BY
    First_Group_Purchase_Date
LIMIT
    6;