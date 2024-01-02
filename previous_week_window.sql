WITH
    payments
AS (
    SELECT
        p.void_date,
        p.used_date, 
        p.issue_date,
        p.settlement_date,
        p.delivery_method,
        p.is_success,
        p.amount,
        p.status,
        count(s.vcard_info_id) AS number_of_swipes,
        sum(s.settled_amount) AS settled_amount
    FROM
        payment_fct p
    JOIN
        settlement_fct s
        USING (id)
    WHERE
        True
        AND p.issue_date >= date '2023-01-01'
        AND s.type IN (1, 2, 3)
    GROUP BY
        1, 2, 3, 4, 5, 6, 7,8
    ),
    issued
AS (
    SELECT
        issue_date AS date,
        delivery_method,
        SUM(amount) AS issued
    FROM 
        payments
    WHERE 
        True 
    GROUP BY 
        1, 2
    ),
    swiped
AS (
    SELECT
        used_date AS date,
        delivery_method,
        SUM(amount) AS swiped
    FROM 
        payments
    WHERE 
        True 
        AND status = 'Used'
    GROUP BY 
        1, 2
    ),
    voided_tpv
AS (
    SELECT
        void_date AS date,
        delivery_method,
        SUM(amount) AS void
    FROM 
        payments
    WHERE 
        True 
        AND status = 'Void'
    GROUP BY 
        1, 2
    ),
    settled_tpv
AS (
    SELECT
        settlement_date AS date,
        delivery_method,
        SUM(amount) AS settled
    FROM 
        payments
    WHERE 
        True 
        AND is_success
    GROUP BY 
        1, 2
    ),
    date_stats 
AS (
    SELECT
        date,
        delivery_method, 
        issued,
        swiped,
        void, 
        settled,
        SUM(issued) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS issued_over_1,
        SUM(issued) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS issued_over_4,
        SUM(issued) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS issued_over_12,
        SUM(swiped) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS swiped_over_1,
        SUM(swiped) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS swiped_over_4,
        SUM(swiped) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS swiped_over_12,
        SUM(void) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS void_over_1,
        SUM(void) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS void_over_4,
        SUM(void) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS void_over_12,
        SUM(settled) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS settled_over_1,
        SUM(settled) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS settled_over_4,
        SUM(settled) OVER (
            PARTITION BY EXTRACT(DOW FROM date), delivery_method
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING       
        ) settled_over_12
    FROM
        issued_tpv
    FULL JOIN 
        swiped_tpv
        USING (date, delivery_method)
    FULL JOIN 
        voided_tpv
        USING (date, delivery_method)
    FULL JOIN 
        settled_tpv
        USING (date, delivery_method)
    ),
    dates 
AS (-- extract previous week data 
    SELECT
        full_date,
        day_name
    FROM
        date_dim
    WHERE
        True 
        AND full_date BETWEEN 
                            date_add('day', -EXTRACT(DOW FROM current_date) - 7, current_date) 
                        AND date_add('day', -EXTRACT(DOW FROM current_date) - 1, current_date)
)

SELECT
    d.full_date,
    d.day_name, 
    s.* 
FROM 
    dates d
JOIN 
    date_stats s
    ON s.date = d.full_date 
ORDER BY 
    1
