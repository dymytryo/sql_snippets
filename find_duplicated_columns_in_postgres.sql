SELECT
    table_name,
    column_name,
    COUNT(*) AS occurences
FROM
    postgres_db.information_schema.columns"
WHERE 
    True 
GROUP BY 
    1,2 
HAVING 
    True 
    AND COUNT(*) > 1;
