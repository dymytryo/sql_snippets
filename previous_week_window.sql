WITH
    payments
AS (
    SELECT
            ...

    ),
    issued
AS (
    SELECT
        issue_date AS date,
        type,
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
        type,
        SUM(amount) AS swiped
    FROM 
        payments
    WHERE 
        True 
        AND status = 'Used'
    GROUP BY 
        1, 2
    ),
    voided_volume
AS (
    SELECT
        void_date AS date,
        type,
        SUM(amount) AS void
    FROM 
        payments
    WHERE 
        True 
        AND status = 'Void'
    GROUP BY 
        1, 2
    ),
    processed_volume
AS (
    SELECT
        settlement_date AS date,
        type,
        SUM(amount) AS processed
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
        type, 
        issued,
        swiped,
        void, 
        processed,
        SUM(issued) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS issued_over_1,
        SUM(issued) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS issued_over_4,
        SUM(issued) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS issued_over_12,
        SUM(swiped) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS swiped_over_1,
        SUM(swiped) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS swiped_over_4,
        SUM(swiped) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS swiped_over_12,
        SUM(void) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS void_over_1,
        SUM(void) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS void_over_4,
        SUM(void) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
        ) AS void_over_12,
        SUM(processed) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
        ) AS processed_over_1,
        SUM(processed) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS processed_over_4,
        SUM(processed) OVER (
            PARTITION BY EXTRACT(DOW FROM date), type
            ORDER BY date
            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING       
        ) processed_over_12
    FROM
        issued_volume
    FULL JOIN 
        swiped_volume
        USING (date, type)
    FULL JOIN 
        voided_volume
        USING (date, type)
    FULL JOIN 
        processed_volume
        USING (date, type)
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
