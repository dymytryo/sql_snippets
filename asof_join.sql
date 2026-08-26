~~~sql
...
FROM
  b
ASOF JOIN
  a
MATCH CONDITION (
  a.date >= b.date) -- for each lefr row, return the single closes righ now that satisfies one inequality
ON 
  b.uuid = a.uuid
~~~~

Alternative, with `QUALIFY`:
~~~sql
...
FROM
  b
LEFT JOIN
  a 
ON 
  b.uuid = a.uuid
  AND a.date >= b.date
WHERE
  True
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY b.uuid, b.date 
                         ORDER BY b.date DESC) = 1
~~~
