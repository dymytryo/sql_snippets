WITH
    month
AS (-- Get all months in the system
    SELECT DISTINCT
        date_trunc('month', full_date) AS month
    FROM
        date_dim
    WHERE
        True
),
    month_pairs
AS (-- Define consecutive month pairs
    SELECT
        m0.month AS month0,
        m1.month AS month1
    FROM
        month m0
    JOIN
        month m1
        ON date_add('month', 1, m0.month) = m1.month
),
    units
AS (-- Aggregate data for each company and month
    SELECT
        cs.company_id,
        ca.channel,
        cs.payment_month                AS trans_month,
        cs.cohort,
        min(cs.payment_date)            AS first_payment_date_in_trans_month,
        max(cs.payment_date)            AS last_payment_date_in_trans_month,
        round(sum(cs.amount), 2)        AS total_rev,
        count(DISTINCT cs.company_id)   AS paying_unit
    FROM
        company_summary cs
    LEFT JOIN
        company_attributes ca
        ON ca.company_id = cs.company_id
        AND cs.payment_month >= ca.record_valid_start
        AND cs.payment_month < ca.record_valid_end
    GROUP BY
        1, 2, 3, 4
    HAVING
        round(sum(total_rev), 2) <> 0
)

-- Main query to calculate metrics for each type of unit activity
SELECT
    month0                                                 AS prev_month,
    month1                                                 AS reporting_month,
    'Beginning'                                            AS type,
    channel,
    DATEADD(month, 1, first_payment_date_in_trans_month)   AS reporting_date,
    sum(paying_unit)                                       AS units
FROM
    units
JOIN
    month_pairs ON trans_month = month0
AND
    DATEADD(month, 1, first_payment_date_in_trans_month) <= current_date
GROUP BY
    1, 2, 3, 4, 5

UNION ALL

SELECT
    month0                                                  AS prev_month,
    month1                                                  AS reporting_month,
    'New'                                                   AS type,
    u1.channel,
    u1.first_payment_date_in_trans_month                    AS reporting_date,
    sum(u1.paying_unit)                                     AS units
FROM
    units u1
JOIN
    month_pairs ON u1.trans_month = month1
LEFT JOIN
    units u0
    ON u1.company_id = u0.company_id
    AND u0.trans_month = month0
WHERE
    True
    AND u0.company_id IS NULL
GROUP BY
    1, 2, 3, 4, 5

UNION ALL

SELECT
    month0                                                   AS prev_month,
    month1                                                   AS reporting_month,
    'Revived'                                                AS type,
    u1.channel,
    u1.first_payment_date_in_trans_month                     AS reporting_date,
    sum(u1.paying_unit)                                      AS units
FROM
    units u1
JOIN
    month_pairs
    ON u1.trans_month = month1
LEFT JOIN
    units u0
    ON u1.company_id = u0.company_id
    AND u0.trans_month = month0
WHERE
    True
    AND u0.company_id IS NULL
    AND u1.first_payment_date_in_trans_month <= month0
GROUP BY
    1, 2, 3, 4, 5

UNION ALL

SELECT
    m.month0                                                AS prev_month,
    m.month1                                                AS reporting_month,
    'Channel Transfer In'                                   AS type,
    u1.channel,
    u1.first_payment_date_in_trans_month                    AS reporting_date,
    sum(u0.paying_unit)                                     AS units
FROM
    units u1
JOIN
    units u0
    ON u1.company_id = u0.company_id
JOIN
    month_pairs m
    ON u1.trans_month = m.month1
    AND u0.trans_month = m.month0
WHERE
    True
    AND u1.channel <> u0.channel
GROUP BY
    1, 2, 3, 4, 5

UNION ALL

SELECT
    month0                                                  AS prev_month,
    month1                                                  AS reporting_month,
    'Channel Transfer Out'                                  AS type,
    u0.channel,
    DATEADD(month, 1, u0.first_payment_date_in_trans_month) AS reporting_date,
    -sum(u0.paying_unit)                                    AS units
FROM
    units u1
JOIN
    units u0
    ON u1.company_id = u0.company_id
JOIN
    month_pairs
    ON u1.trans_month = month1
    AND u0.trans_month = month0
WHERE
    True
    AND u1.channel <> u0.channel
    AND DATEADD(month, 1, 
  u0.last_payment_date_in_trans_month) <= current_date
GROUP BY
    1, 2, 3, 4, 5

UNION ALL

SELECT
    month0                                                  AS prev_month,
    month1                                                  AS reporting_month,
    'Attrition'                                             AS type,
    u0.channel,
    DATEADD(month, 1, u0.last_payment_date_in_trans_month)  AS reporting_date,
    -sum(u0.paying_unit)                                    AS units
FROM
    units u0
JOIN
    month_pairs
    ON u0.trans_month = month0
LEFT JOIN
    units u1
    ON u1.company_id = u0.company_id
    AND u1.trans_month = month1
WHERE
    True
    AND u1.company_id IS NULL
    AND DATEADD(month, 1,
  u0.last_payment_date_in_trans_month) <= current_date
GROUP BY
    1, 2, 3, 4, 5

UNION ALL

SELECT
    m.month0                                                AS prev_month,
    m.month1                                                AS reporting_month,
    'Ending'                                                AS type,
    u.channel,
    u.first_payment_date_in_trans_month                     AS reporting_date,
    sum(u.paying_unit)                                      AS units
FROM
    units u
JOIN
    month_pairs m
    ON m.month1 = u.trans_month
GROUP BY
    1, 2, 3, 4, 5;
