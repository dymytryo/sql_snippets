    split(regexp_replace(regexp_replace(trim(lower(name)), '\[|\]|'''), '(\w)(\w*)',
        x -> upper(x[1]) || lower(x[2])), ',')[1]   AS "Vendor Name",


SELECT 
    merchant_id,
    first_value(name) IGNORE NULLS OVER (PARTITION BY merchant_id ORDER BY occurences DESC) AS name
FROM
    (
    SELECT
        mn.id AS merchant_id,
        TRIM(
            regexp_replace(
            regexp_replace(
            regexp_replace(lower(mn.name),
                            '(?i)llc|inc|dba|deleted|co'),
                            '[.,/#!$%^*;:=_`~()-]'),
                            '[0-9]')
                            ) AS name,
        COUNT(mn.form) AS  occurences
    FROM
        merchantName mn
    WHERE
        True
    GROUP BY
      1)
