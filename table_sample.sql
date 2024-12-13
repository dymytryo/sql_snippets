-- check the behavior in the subset of data, but leverage sampling to scan less data 
SELECT
    p.id                             AS merchant_profile,
    count(distinct m.id)             AS unique_merchants,
    count(distinct b.invoiceNumber)  AS unique_invoices
FROM
    (
    SELECT
        id
    FROM
        merchants
    WHERE
        True
        AND isActive = 1
     ) m TABLESAMPLE
                    BERNOULLI (10)      
JOIN
    bill b
    ON b.merchantId = m.id
    AND b.partition_id >= '2024-01-01'
JOIN
    profiles p
    ON p.id = m.profileId
WHERE
   True
GROUP BY
    1
