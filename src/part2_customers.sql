DROP TABLE IF EXISTS segments;
CREATE TABLE IF NOT EXISTS segments (
    segment SERIAL PRIMARY KEY,
    average_check VARCHAR,
    frequency_of_purchases VARCHAR,
    churn_probability VARCHAR
);
COPY segments (
    segment,
    average_check,
    frequency_of_purchases,
    churn_probability
)
FROM
    '/tmp/import/segments.csv' DELIMITER ',' CSV;
-- Для Windows
COPY segments (
    segment,
    average_check,
    frequency_of_purchases,
    churn_probability
)
FROM
    'C:\datasets\segments.csv' DELIMITER ',' CSV;
DROP FUNCTION if EXISTS primary_store;
CREATE
OR REPLACE FUNCTION primary_store(cust_id INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN (
    WITH preliminary AS (
        SELECT
            t.transaction_store_id,
            COUNT(*) OVER (PARTITION BY t.transaction_store_id) AS store_visits,
            MAX(t.transaction_datetime) OVER (PARTITION BY t.transaction_store_id) AS closest_date,
            ROW_NUMBER() OVER (
                ORDER BY
                    t.transaction_datetime DESC
            ) AS order_store
        FROM
            personal_information p
            JOIN cards c USING(customer_id)
            JOIN transactions t USING (customer_card_id)
        WHERE
            p.customer_id = cust_id
    ),
    on_the_ball AS (
        SELECT
            DISTINCT FIRST_VALUE(p.transaction_store_id) OVER (
                ORDER BY
                    p.store_visits DESC,
                    p.closest_date DESC
            ) AS the_best
        FROM
            preliminary p
    ),
    fresh_visits AS (
        SELECT
            DISTINCT MAX(p.transaction_store_id) AS fresh_id,
            MIN(p.transaction_store_id) = MAX(p.transaction_store_id) AS answer
        FROM
            preliminary p
        WHERE
            p.order_store <= 3
    )
    SELECT
        CASE
            WHEN (
                SELECT
                    answer
                FROM
                    fresh_visits
            ) THEN (
                SELECT
                    fresh_id
                FROM
                    fresh_visits
            )
            ELSE (
                SELECT
                    the_best
                FROM
                    on_the_ball
            )
        END AS store_id
);
END;
$$;
DROP VIEW IF EXISTS customers;
CREATE
OR REPLACE VIEW customers AS (
    WITH checks AS (
        SELECT
            c.customer_id,
            round(
                SUM(t.transaction_summ) / COUNT(t.transaction_summ),
                8
            ) AS customer_average_check
        FROM
            cards c
            JOIN transactions t USING(customer_card_id)
        GROUP BY
            c.customer_id
        ORDER BY
            c.customer_id
    ),
    cume AS (
        SELECT
            c.customer_id,
            c.customer_average_check,
            CUME_DIST() OVER (
                ORDER BY
                    customer_average_check desc
            ) AS cume_check
        FROM
            checks c
        ORDER BY
            c.customer_id
    ),
    cume_end AS (
        SELECT
            c.customer_id,
            c.customer_average_check,
            CASE
                WHEN cume_check <= 0.1 THEN 'High'
                WHEN cume_check > 0.1
                AND cume_check <= 0.35 THEN 'Medium'
                ELSE 'Low'
            END AS customer_average_check_segment
        FROM
            cume c
        ORDER BY
            c.customer_id
    ),
    intervals AS (
        SELECT
            c.customer_id,
            (
                MAX(t.transaction_datetime) - MIN(t.transaction_datetime)
            ) / COUNT(t.transaction_id) AS customer_frequency
        FROM
            cards c
            JOIN transactions t USING(customer_card_id)
        GROUP BY
            c.customer_id
        ORDER BY
            c.customer_id
    ),
    cume_freq AS (
        SELECT
            i.customer_id,
            i.customer_frequency,
            CUME_DIST() OVER (
                ORDER BY
                    i.customer_frequency
            ) AS cume_freq
        FROM
            intervals i
        ORDER BY
            i.customer_id
    ),
    frequency AS (
        SELECT
            f.customer_id,
            round(
                EXTRACT(
                    epoch
                    FROM
                        f.customer_frequency
                ) / 3600 / 24,
                10
            ) AS customer_frequency,
            CASE
                WHEN f.cume_freq <= 0.1 THEN 'Often'
                WHEN f.cume_freq > 0.1
                AND f.cume_freq <= 0.35 THEN 'Occasionally'
                ELSE 'Rarely'
            END AS customer_frequency_segment
        FROM
            cume_freq f
        ORDER BY
            f.customer_id
    ),
    term AS (
        SELECT
            c.customer_id,
            round(
                EXTRACT(
                    epoch
                    FROM
                        (
                            (
                                SELECT
                                    *
                                FROM
                                    date_of_analysis_formation
                            ) - MAX(t.transaction_datetime)
                        )
                ) / 3600 / 24,
                10
            ) AS customer_inactive_period
        FROM
            cards c
            JOIN transactions t USING(customer_card_id)
        GROUP BY
            1
        ORDER BY
            1
    ),
    rate AS (
        SELECT
            f.customer_id,
            round(
                t.customer_inactive_period / f.customer_frequency,
                10
            ) AS customer_churn_rate
        FROM
            term t
            INNER JOIN frequency f USING(customer_id)
        ORDER BY
            f.customer_id
    ),
    pred AS (
        SELECT
            r.customer_id,
            c.customer_average_check,
            c.customer_average_check_segment,
            f.customer_frequency,
            f.customer_frequency_segment,
            t.customer_inactive_period,
            r.customer_churn_rate,
            CASE
                WHEN r.customer_churn_rate <= 2 THEN 'Low'
                WHEN r.customer_churn_rate > 2
                AND r.customer_churn_rate <= 5 THEN 'Medium'
                ELSE 'High'
            END AS customer_churn_segment
        FROM
            rate r
            JOIN frequency f USING (customer_id)
            JOIN term t USING (customer_id)
            JOIN cume_end c USING (customer_id)
        ORDER BY
            c.customer_id
    )
    SELECT
        p.customer_id,
        p.customer_average_check,
        p.customer_average_check_segment,
        p.customer_frequency,
        p.customer_frequency_segment,
        p.customer_inactive_period,
        p.customer_churn_rate,
        p.customer_churn_segment,
        s.segment AS Customer_Segment,
        primary_store(p.customer_id) AS Customer_Primary_Store
    FROM
        pred p
        LEFT JOIN segments s ON p.Customer_Average_Check_Segment = s.Average_check
        AND p.Customer_Frequency_Segment = s.Frequency_of_purchases
        AND p.Customer_Churn_Segment = s.Churn_probability
    ORDER BY
        customer_id
);
SELECT
    customer_id,
    customer_average_check,
    customer_average_check_segment
FROM
    customers
ORDER BY
    customer_average_check desc;
SELECT
    customer_id,
    customer_frequency,
    customer_frequency_segment
FROM
    customers
ORDER BY
    2;
SELECT
    customer_id,
    customer_inactive_period,
    customer_churn_rate,
    customer_churn_segment
FROM
    customers
ORDER BY
    3 desc;
SELECT
    customer_id,
    customer_average_check_segment,
    customer_frequency_segment,
    customer_churn_segment,
    Customer_Segment
FROM
    customers
ORDER BY
    5;
SELECT
    customer_id,
    Customer_Primary_Store
FROM
    customers;