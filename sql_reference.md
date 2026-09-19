# SQL reference

Structured Query Language (SQL) is a declarative language for defining, reading,
changing, and controlling relational data. A SQL statement describes the result or
state change required; the database engine chooses a physical execution plan.

Portable SQL is the default in this guide. Examples that depend on PostgreSQL,
Amazon Redshift, Trino/Starburst, Snowflake, MySQL, or BigQuery are labeled where
they appear. Function names and argument order are not portable merely because
several engines use similar names.

```text
SQL text
   |
   v
parse and validate names, types, and permissions
   |
   v
logical plan: scan -> filter -> join -> aggregate -> window -> project
   |
   v
optimizer chooses access paths, join order, and data movement
   |
   v
workers read data and compute the result or state change
```

| Concern | Behavior |
|---|---|
| Storage | Tables persist data; views persist a query definition; temporary objects follow engine-specific lifetimes. |
| Compute and cost | Scanned data, joins, sorts, shuffles, and materialization usually dominate. Column pruning, selective predicates, and suitable table design reduce work. |
| Security | The active user or role needs privileges for every object read or changed. Views can expose a controlled projection when the engine supports the required security model. |
| Transactions | Transaction support and isolation vary by engine and table format. `COMMIT` makes a transaction durable; `ROLLBACK` abandons its uncommitted changes. |
| Portability | Core relational operations transfer well. Types, date functions, regular expressions, semi-structured data, procedures, and maintenance commands vary substantially. |

## Contents

- Foundations: [core terms](#core-terms), [example data](#example-data), and
  [logical query processing](#logical-query-processing).
- Querying: [read and shape rows](#read-and-shape-rows),
  [expressions and operators](#literals-expressions-and-operators),
  [predicates](#filter-rows-with-predicates), [joins](#join-relations),
  [set operations](#combine-result-sets), [aggregation](#aggregate-and-group-rows),
  [subqueries and CTEs](#subqueries-and-common-table-expressions), and
  [window functions](#window-functions).
- Values: [strings](#string-functions-and-cleanup),
  [dates and time zones](#dates-timestamps-and-time-zones), and
  [arrays, maps, and JSON](#arrays-maps-json-and-complex-types).
- State changes: [DML](#change-rows-with-dml), [tables and types](#define-tables-and-types),
  [object changes](#change-and-remove-objects), [views](#views),
  [programmable objects](#programmable-objects), and
  [access and transactions](#control-access-and-transactions).
- Operations: [data quality](#data-quality-recipes),
  [sampling](#sampling-and-approximate-analysis),
  [bit operations](#bit-operations-and-fingerprints),
  [metadata](#inspect-types-and-object-definitions),
  [maintenance](#engine-specific-maintenance), and
  [related platform behavior](#related-platform-behavior).

## Core terms

| Term | Meaning |
|---|---|
| Relation | A table-shaped set of rows. A table, view, common table expression, or query result can be treated as a relation. |
| Row | One record in a relation. |
| Column | A named, typed attribute of a relation. |
| Predicate | An expression evaluated as `TRUE`, `FALSE`, or `UNKNOWN`, normally used to filter or join rows. |
| Projection | The expressions returned by the `SELECT` list. |
| Cardinality | The number of rows in a relation or intermediate result. |
| DDL | Data Definition Language: statements such as `CREATE`, `ALTER`, `DROP`, and `TRUNCATE` that create, change, remove, or clear database objects. |
| DML | Data Manipulation Language: statements such as `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `MERGE` that read or change rows. |
| DCL | Data Control Language: statements such as `GRANT` and `REVOKE` that manage privileges. |
| TCL | Transaction Control Language: statements such as `COMMIT`, `ROLLBACK`, and `SAVEPOINT` that manage transactions. |
| CRUD | Create, read, update, and delete, the four basic operations applied to stored records. |
| ETL | Extract, transform, and load: moving source data, changing its shape or meaning, and writing it to a destination. |

Classification is a teaching convention rather than executable syntax. Some references
place `SELECT` under Data Query Language instead of DML, and engines differ in how they
classify transaction and maintenance statements.

## Example data

The introductory examples use these rows so their results remain concrete.

`customers`:

| `id` | `first_name` | `last_name` | `city` | `age` |
|---:|---|---|---|---:|
| 1 | Ava | Cole | New York | 30 |
| 2 | Ben | O'Neil | Chicago | 35 |
| 3 | Cara | Diaz | Los Angeles | 28 |
| 4 | Dev | Shah | New York | 35 |
| 5 | Emi | Wang | Chicago | 30 |
| 6 | Finn | Reed | Boston | 41 |

`employees`:

| `employee_id` | `first_name` | `last_name` | `department_id` | `salary` |
|---:|---|---|---:|---:|
| 10 | Ana | Bell | 100 | 3000 |
| 11 | Bo | Chen | 100 | 3500 |
| 12 | Cy | Dube | 200 | 4200 |
| 13 | Dee | Evans | 200 | 4200 |

`orders`:

| `order_id` | `customer_id` | `amount` | `order_date` |
|---:|---:|---:|---|
| 101 | 1 | 75.00 | 2026-02-01 |
| 102 | 1 | 125.00 | 2026-02-03 |
| 103 | 2 | 40.00 | 2026-02-03 |
| 104 | 4 | 90.00 | 2026-02-05 |

## Logical query processing

SQL is written with `SELECT` near the top, but the logical processing order begins
with the source relations. The optimizer may execute a physically equivalent plan in
a different order.

```text
FROM and JOIN
      |
      v
WHERE
      |
      v
GROUP BY
      |
      v
HAVING
      |
      v
window functions
      |
      v
SELECT
      |
      v
DISTINCT
      |
      v
set operation: UNION / INTERSECT / EXCEPT
      |
      v
ORDER BY
      |
      v
LIMIT and OFFSET
```

This explains several common rules:

- `WHERE` cannot normally refer to a `SELECT` alias because filtering happens first.
- `HAVING` filters groups after aggregation.
- Window functions see the rows left by `WHERE`, grouping, and `HAVING`.
- `ORDER BY` controls presentation order; earlier operations do not guarantee it.

## Read and shape rows

### Complete statements

A semicolon terminates a statement. It is especially important when a script contains
more than one statement.

```sql
SELECT
  first_name
FROM
  customers
ORDER BY
  id;

SELECT
  city
FROM
  customers
ORDER BY
  id;
```

First result:

| `first_name` |
|---|
| Ava |
| Ben |
| Cara |
| Dev |
| Emi |
| Finn |

Second result:

| `city` |
|---|
| New York |
| Chicago |
| Los Angeles |
| New York |
| Chicago |
| Boston |

SQL uses `--` for a line comment and `/* ... */` for a block comment:

```sql
-- Explain why this predicate exists.

/*
Multi-line explanation or a temporarily disabled fragment.
*/
```

`//` and `#` are not portable SQL comment markers.

### Select every column

`*` projects every column visible from the source relation.

```sql
SELECT
  *
FROM
  customers
ORDER BY
  id;
```

| `id` | `first_name` | `last_name` | `city` | `age` |
|---:|---|---|---|---:|
| 1 | Ava | Cole | New York | 30 |
| 2 | Ben | O'Neil | Chicago | 35 |
| 3 | Cara | Diaz | Los Angeles | 28 |
| 4 | Dev | Shah | New York | 35 |
| 5 | Emi | Wang | Chicago | 30 |
| 6 | Finn | Reed | Boston | 41 |

`SELECT *` is convenient for exploration. Production queries should normally name
their columns so schema changes do not silently change the output contract or scan
unneeded data.

### Remove duplicate result rows

`DISTINCT` keeps one copy of each distinct combination in the projection.

```sql
SELECT DISTINCT
  city
FROM
  customers
ORDER BY
  city;
```

| `city` |
|---|
| Boston |
| Chicago |
| Los Angeles |
| New York |

### Rename output columns and relations

`AS` assigns an alias. Column aliases name output expressions; table aliases shorten
qualified references. Most engines allow `AS` for columns, while support for `AS` on
table aliases is nearly universal but not completely identical.

```sql
SELECT
  c.first_name,
  c.city AS customer_city
FROM
  customers AS c
WHERE
  c.id = 1;
```

| `first_name` | `customer_city` |
|---|---|
| Ava | New York |

### Sort rows

`ORDER BY ... ASC` sorts ascending; `DESC` sorts descending. Ascending is the usual
default, but writing the direction makes intent explicit.

```sql
SELECT
  first_name,
  salary
FROM
  employees
WHERE
  salary > 3100
ORDER BY
  salary DESC,
  first_name ASC;
```

| `first_name` | `salary` |
|---|---:|
| Cy | 4200 |
| Dee | 4200 |
| Bo | 3500 |

Without `ORDER BY`, a database may return rows in any order.

### Limit and skip rows

`LIMIT` caps the number of returned rows. `OFFSET` skips rows after ordering. Use a
deterministic `ORDER BY`; otherwise the selected page is not stable.

```sql
SELECT
  id,
  first_name,
  last_name,
  city
FROM
  customers
ORDER BY
  id
LIMIT 5;
```

| `id` | `first_name` | `last_name` | `city` |
|---:|---|---|---|
| 1 | Ava | Cole | New York |
| 2 | Ben | O'Neil | Chicago |
| 3 | Cara | Diaz | Los Angeles |
| 4 | Dev | Shah | New York |
| 5 | Emi | Wang | Chicago |

This example uses Trino's `OFFSET`-then-`LIMIT` clause order:

```sql
SELECT
  id
FROM
  customers
ORDER BY
  id
OFFSET 3
LIMIT 4;
```

| `id` |
|---:|
| 4 |
| 5 |
| 6 |

`LIMIT` and `OFFSET` are common but not universal SQL syntax. Some engines use
`FETCH FIRST`, `TOP`, or other pagination forms. Large offsets can be expensive
because the engine still finds and discards the preceding rows.

## Literals, expressions, and operators

### Text literals

Text literals use single quotation marks. An apostrophe inside a literal is escaped
by doubling it.

```sql
SELECT
  'Can''t' AS escaped_text;
```

| `escaped_text` |
|---|
| Can't |

```sql
SELECT
  id,
  first_name,
  last_name,
  city
FROM
  customers
WHERE
  city = 'New York'
ORDER BY
  id;
```

| `id` | `first_name` | `last_name` | `city` |
|---:|---|---|---|
| 1 | Ava | Cole | New York |
| 4 | Dev | Shah | New York |

Identifiers use engine-specific quoting, commonly double quotes. Do not use quoted
text interpolation to build SQL from untrusted values; use bind parameters.

### Comparison operators

| Operator | Meaning |
|---|---|
| `=` | Equal |
| `<>` or `!=` | Not equal |
| `<`, `<=` | Less than, or less than or equal |
| `>`, `>=` | Greater than, or greater than or equal |
| `IS NULL`, `IS NOT NULL` | Test for the absence or presence of a value |
| `IS DISTINCT FROM` | Null-safe inequality in engines that support it |
| `IS NOT DISTINCT FROM` | Null-safe equality in engines that support it |

Standard SQL equality is `=`, not `==`. A few engines accept `==` as an extension.

### Logical operators and parentheses

`AND` is true only when both predicates are true. `OR` is true when either predicate
is true. `NOT` negates a predicate. `AND` normally binds more tightly than `OR`, but
parentheses should make the intended grouping explicit.

```sql
SELECT
  id,
  first_name,
  city,
  age
FROM
  customers
WHERE
  city = 'New York'
  AND (age = 30 OR age = 35)
ORDER BY
  id;
```

| `id` | `first_name` | `city` | `age` |
|---:|---|---|---:|
| 1 | Ava | New York | 30 |
| 4 | Dev | New York | 35 |

### Arithmetic operators

The portable arithmetic operators are `+`, `-`, `*`, and `/`. Modulo syntax varies;
many engines use `%` or `MOD(x, y)`. Numeric type rules determine whether division is
integer or fractional.

```sql
SELECT
  employee_id,
  first_name,
  salary + 500 AS adjusted_salary
FROM
  employees
ORDER BY
  employee_id;
```

| `employee_id` | `first_name` | `adjusted_salary` |
|---:|---|---:|
| 10 | Ana | 3500 |
| 11 | Bo | 4000 |
| 12 | Cy | 4700 |
| 13 | Dee | 4700 |

### Three-valued logic and `NULL`

`NULL` represents a missing or unknown value. Most comparisons involving `NULL`
evaluate to `UNKNOWN`, not `TRUE` or `FALSE`. `WHERE` retains only rows for which its
predicate is `TRUE`.

```sql
SELECT
  NULL = NULL AS ordinary_equality,
  NULL IS DISTINCT FROM NULL AS null_safe_inequality,
  NULL IS NOT DISTINCT FROM NULL AS null_safe_equality;
```

| `ordinary_equality` | `null_safe_inequality` | `null_safe_equality` |
|---|---|---|
| `NULL` | `FALSE` | `TRUE` |

Account for a nullable prior value explicitly:

```sql
SELECT
  id,
  old_value,
  new_value
FROM
  status_changes
WHERE
  new_value = 1
  AND (old_value <> 1 OR old_value IS NULL);
```

Representative result:

| `id` | `old_value` | `new_value` |
|---:|---:|---:|
| 7 | `NULL` | 1 |
| 9 | 0 | 1 |

`NOT IN` needs special care: if its candidate set contains `NULL`, the predicate can
become `UNKNOWN` for every unmatched row. Filter nulls from the subquery or prefer a
`NOT EXISTS` anti-join.

### Replace `NULL` values

`COALESCE` returns the first non-null argument.

```sql
SELECT
  COALESCE(NULL, NULL, 'W3Schools.com', 'Example.com') AS first_present_value;
```

| `first_present_value` |
|---|
| W3Schools.com |

Portable conditional logic uses `CASE`. Snowflake also supplies `IFF` for a single
if-then-else expression.

```sql
SELECT
  CASE
    WHEN category = 'Required' THEN 'Required'
    ELSE 'Optional'
  END AS requirement
FROM
  tasks
ORDER BY
  requirement;
```

Representative result:

| `requirement` |
|---|
| Optional |
| Required |

Snowflake equivalent:

```sql
SELECT
  IFF(TRUE, 'true', 'false') AS result;
```

| `result` |
|---|
| true |

## Filter rows with predicates

### Membership with `IN`

`IN` compares one expression with a list or a one-column subquery. `NOT IN` negates
the test, subject to the `NULL` behavior described above.

```sql
SELECT
  id,
  first_name,
  city
FROM
  customers
WHERE
  city IN ('New York', 'Los Angeles', 'Chicago')
ORDER BY
  id;
```

| `id` | `first_name` | `city` |
|---:|---|---|
| 1 | Ava | New York |
| 2 | Ben | Chicago |
| 3 | Cara | Los Angeles |
| 4 | Dev | New York |
| 5 | Emi | Chicago |

A subquery used by `IN` must return exactly one column.

```sql
SELECT
  name
FROM
  nation
WHERE
  region_key IN (
    SELECT
      region_key
    FROM
      region
  )
ORDER BY
  name;
```

Representative result:

| `name` |
|---|
| Canada |
| Mexico |
| United States |

### Pattern matching with `LIKE`

`LIKE` uses `%` for any sequence of characters and `_` for exactly one character.

```sql
SELECT
  first_name
FROM
  customers
WHERE
  first_name LIKE 'A%';
```

| `first_name` |
|---|
| Ava |

`'_p%'` means any one character, followed by `p`, followed by any suffix:

```sql
SELECT
  username
FROM
  users
WHERE
  username LIKE '_p%';
```

Representative result:

| `username` |
|---|
| april |
| spike |

Use `ESCAPE` when `%` or `_` should be literal characters.

```sql
SELECT
  code
FROM
  products
WHERE
  code LIKE 'se#_%' ESCAPE '#';
```

Representative result:

| `code` |
|---|
| se_100 |

PostgreSQL and Redshift provide `ILIKE` for case-insensitive matching. Collations and
other engines provide different case-sensitivity rules.

### Regular expressions

Regular-expression syntax and matching semantics are engine-specific. In Trino,
`regexp_like` tests whether the pattern occurs anywhere unless it is anchored.
For a fixed list of exact literals, `IN (...)` is simpler than a regular expression.

```sql
SELECT
  value
FROM
  labels
WHERE
  regexp_like(value, '^(match1|match2|match3)$');
```

Representative result:

| `value` |
|---|
| match1 |
| match3 |

The trailing alternative in `'^(match1|match2|match3|)$'` would also match an empty
string, so omit it unless that behavior is intentional.

PostgreSQL uses `~` for a case-sensitive Portable Operating System Interface (POSIX)
regular expression and `~*` for a case-insensitive one. `SIMILAR TO` uses SQL
regular-expression syntax and matches the
whole string. `LIKE` is simpler and can be cheaper than a regular expression when it
expresses the same predicate.

```sql
SELECT
  'Hello' ~ 'H.*' AS case_sensitive_match,
  'Hello' ~* 'h.*' AS case_insensitive_match;
```

| `case_sensitive_match` | `case_insensitive_match` |
|---|---|
| `TRUE` | `TRUE` |

### `EXISTS`

`EXISTS` is true when its subquery returns at least one row. A correlated subquery can
refer to the outer row and is evaluated semantically for each outer row, although the
optimizer may transform it into a join.

```sql
SELECT
  n.name
FROM
  nation AS n
WHERE EXISTS (
  SELECT
    1
  FROM
    region AS r
  WHERE
    r.region_key = n.region_key
)
ORDER BY
  n.name;
```

Representative result:

| `name` |
|---|
| Canada |
| Mexico |
| United States |

### Scalar subqueries

A scalar subquery returns one column and at most one row. Zero rows produce `NULL`;
more than one row is an error.

```sql
SELECT
  first_name,
  age
FROM
  customers
WHERE
  age = (
    SELECT
      MAX(age)
    FROM
      customers
  )
ORDER BY
  first_name;
```

| `first_name` | `age` |
|---|---:|
| Finn | 41 |

This salary query uses a non-correlated scalar subquery because the inner query does
not reference the outer employee row.

```sql
SELECT
  first_name,
  salary
FROM
  employees
WHERE
  salary > (
    SELECT
      AVG(salary)
    FROM
      employees
  )
ORDER BY
  salary DESC,
  first_name;
```

| `first_name` | `salary` |
|---|---:|
| Cy | 4200 |
| Dee | 4200 |

## Join relations

A join combines rows from two relations. Prefer explicit join syntax over the
older comma-separated form because the relationship is visible in the `ON` clause.

| Join | Rows retained |
|---|---|
| `INNER JOIN` | Only pairs that satisfy the join predicate. |
| `LEFT JOIN` | Every left row; unmatched right columns become `NULL`. |
| `RIGHT JOIN` | Every right row; unmatched left columns become `NULL`. |
| `FULL JOIN` | Every row from both sides; the unmatched side becomes `NULL`. |
| `CROSS JOIN` | Every left row paired with every right row. |

```sql
SELECT
  c.id,
  c.first_name,
  o.order_id,
  o.amount
FROM
  customers AS c
INNER JOIN
  orders AS o
  ON o.customer_id = c.id
ORDER BY
  c.id,
  o.order_id;
```

| `id` | `first_name` | `order_id` | `amount` |
|---:|---|---:|---:|
| 1 | Ava | 101 | 75.00 |
| 1 | Ava | 102 | 125.00 |
| 2 | Ben | 103 | 40.00 |
| 4 | Dev | 104 | 90.00 |

The older equivalent is legal but easier to break by omitting its join predicate:

```sql
SELECT
  c.id,
  c.first_name,
  o.order_id,
  o.amount
FROM
  customers AS c,
  orders AS o
WHERE
  o.customer_id = c.id
ORDER BY
  c.id,
  o.order_id;
```

| `id` | `first_name` | `order_id` | `amount` |
|---:|---|---:|---:|
| 1 | Ava | 101 | 75.00 |
| 1 | Ava | 102 | 125.00 |
| 2 | Ben | 103 | 40.00 |
| 4 | Dev | 104 | 90.00 |

### Join with `USING`

`USING` is concise when both inputs use the same join-column name. The result exposes
one merged copy of each `USING` column.

```sql
SELECT
  profile_id,
  b.business_name,
  d.directory_name
FROM
  business_directory AS b
JOIN
  business_directory_names AS d
  USING (profile_id)
ORDER BY
  profile_id
LIMIT 10;
```

Representative result:

| `profile_id` | `business_name` | `directory_name` |
|---:|---|---|
| 42 | Northwind Market | Northwind |

### Cross joins and row explosion

`CROSS JOIN` returns the Cartesian product. If `nation` has 25 rows and `region` has
5, the result has 125 rows.

```sql
SELECT
  n.name AS nation_name,
  r.name AS region_name
FROM
  nation AS n
CROSS JOIN
  region AS r
ORDER BY
  nation_name,
  region_name;
```

Representative result excerpt:

| `nation_name` | `region_name` |
|---|---|
| Algeria | Africa |
| Algeria | America |
| Algeria | Asia |

Unexpected row multiplication usually comes from a missing predicate or a many-to-
many key. Measure key multiplicity before joining:

```sql
SELECT
  id,
  event_timestamp,
  COUNT(*) AS row_count
FROM
  source_events
GROUP BY
  id,
  event_timestamp
HAVING
  COUNT(*) > 1
ORDER BY
  id,
  event_timestamp;
```

Representative result:

| `id` | `event_timestamp` | `row_count` |
|---:|---|---:|
| 17 | 2026-02-03 10:15:00 | 3 |

## Combine result sets

Set operations combine compatible query results vertically. Each input must return
the same number of columns, and corresponding columns must have compatible types.

| Operation | Behavior |
|---|---|
| `UNION ALL` | Appends every row and preserves duplicates. |
| `UNION` | Appends rows, then removes duplicates from the complete combined result. |
| `INTERSECT` | Keeps rows present in both results. |
| `EXCEPT` | Keeps rows in the first result that are absent from the second. |

`UNION` removes duplicates within each input as well as duplicates between inputs.
Use `UNION ALL` when duplicate removal is not required; it avoids that extra work.

```sql
SELECT
  city
FROM
  customers
WHERE
  id <= 3

UNION

SELECT
  city
FROM
  customers
WHERE
  id >= 4
ORDER BY
  city;
```

| `city` |
|---|
| Boston |
| Chicago |
| Los Angeles |
| New York |

Use a typed `NULL` placeholder when one source lacks a column. An explicit cast may be
necessary if the engine cannot infer a compatible type.

```sql
SELECT
  first_name,
  last_name,
  company
FROM
  business_contacts

UNION ALL

SELECT
  first_name,
  last_name,
  CAST(NULL AS VARCHAR) AS company
FROM
  personal_contacts;
```

Representative result:

| `first_name` | `last_name` | `company` |
|---|---|---|
| Ana | Bell | Acme |
| Bo | Chen | `NULL` |

Parentheses isolate ordering and limiting within each set-operation input:

```sql
(
  SELECT
    id
  FROM
    current_ids
  ORDER BY
    id
  LIMIT 10
)
INTERSECT
(
  SELECT
    id
  FROM
    permitted_ids
  ORDER BY
    id
  LIMIT 10
)
ORDER BY
  id;
```

Representative result:

| `id` |
|---:|
| 10 |
| 12 |

## Aggregate and group rows

Aggregate functions reduce a group of input rows to one value. Without `GROUP BY`,
the entire input is one group. Most aggregates ignore `NULL`; `COUNT(*)` counts rows.

```sql
SELECT
  AVG(salary) AS average_salary,
  SUM(salary) AS total_salary,
  SQRT(16) AS square_root
FROM
  employees;
```

| `average_salary` | `total_salary` | `square_root` |
|---:|---:|---:|
| 3725 | 14900 | 4 |

### `GROUP BY` and `HAVING`

`GROUP BY` forms groups. `HAVING` filters the aggregated groups; `WHERE` filters rows
before they enter an aggregate.

```sql
SELECT
  department_id,
  COUNT(*) AS employee_count,
  AVG(salary) AS average_salary
FROM
  employees
GROUP BY
  department_id
HAVING
  COUNT(*) >= 2
ORDER BY
  department_id;
```

| `department_id` | `employee_count` | `average_salary` |
|---:|---:|---:|
| 100 | 2 | 3250 |
| 200 | 2 | 4200 |

Ordinal grouping such as `GROUP BY 1, 2` is concise but fragile when the select list
is reordered. Naming the expressions is easier to maintain.

### Conditional aggregates with `FILTER`

On engines that support it, `FILTER` applies a predicate to one aggregate without
removing rows from other aggregates.

```sql
SELECT
  SUM(amount) AS total_volume,
  SUM(amount) FILTER (WHERE is_online) AS online_volume,
  ARRAY_AGG(name ORDER BY name)
    FILTER (WHERE name IS NOT NULL) AS present_names
FROM
  payments;
```

Representative result:

| `total_volume` | `online_volume` | `present_names` |
|---:|---:|---|
| 300.00 | 225.00 | `[Ana, Bo]` |

`FILTER` is defined for aggregates. Applying it to a value window function such as
`FIRST_VALUE` is rejected by many engines. Use the engine's `IGNORE NULLS` support or
a filtered aggregate such as `MAX_BY` when that expresses the required behavior.

### String aggregation

`LISTAGG` concatenates values within each group. The exact overflow behavior and
availability of `DISTINCT` vary by engine.

```sql
SELECT
  organization_id,
  reporting_month,
  LISTAGG(
    CONCAT(contact_theme, ':', CAST(ticket_count AS VARCHAR)),
    ', '
  ) WITHIN GROUP (ORDER BY contact_theme) AS themes
FROM
  support_ticket_summary
GROUP BY
  organization_id,
  reporting_month;
```

Representative result:

| `organization_id` | `reporting_month` | `themes` |
|---:|---|---|
| 7 | 2026-02-01 | Billing:4, Login:2 |

### Multiple aggregation levels

`GROUPING SETS` defines several grouping lists in one aggregation. Columns absent from
a grouping set are represented as `NULL` in its subtotal rows.

```sql
SELECT
  r.country_name,
  r.region_name,
  COUNT(*) AS employee_count
FROM
  employees AS e
JOIN
  company_regions AS r
  ON r.id = e.region_id
GROUP BY GROUPING SETS (
  (r.country_name),
  (r.country_name, r.region_name)
)
ORDER BY
  r.country_name,
  r.region_name;
```

Representative result:

| `country_name` | `region_name` | `employee_count` |
|---|---|---:|
| Canada | East | 4 |
| Canada | West | 3 |
| Canada | `NULL` | 7 |
| USA | Northeast | 5 |
| USA | `NULL` | 5 |

`ROLLUP(a, b)` produces hierarchical totals: `(a, b)`, `(a)`, and `()`. `CUBE(a, b)`
produces the power set: `(a, b)`, `(a)`, `(b)`, and `()`. `GROUPING(column)` can
distinguish a subtotal placeholder from a stored `NULL`.

Trino allows `GROUP BY DISTINCT` or `GROUP BY ALL` before grouping sets to control
whether duplicate grouping sets are deduplicated. This is not the same as Snowflake's
separate `GROUP BY ALL` convenience, which groups by every non-aggregate item in the
`SELECT` list.

Snowflake example:

```sql
SELECT
  city,
  age,
  COUNT(*) AS customer_count
FROM
  customers
GROUP BY ALL
ORDER BY
  city,
  age;
```

| `city` | `age` | `customer_count` |
|---|---:|---:|
| Boston | 41 | 1 |
| Chicago | 30 | 1 |
| Chicago | 35 | 1 |
| Los Angeles | 28 | 1 |
| New York | 30 | 1 |
| New York | 35 | 1 |

### Buckets and histograms

`WIDTH_BUCKET(x, lower, upper, count)` assigns values to equal-width numeric buckets.
Boundary behavior is defined by the engine and usually includes underflow and
overflow bucket numbers.

```sql
SELECT
  employee_id,
  salary,
  WIDTH_BUCKET(salary, 0, 5000, 5) AS salary_bucket
FROM
  employees
ORDER BY
  employee_id;
```

| `employee_id` | `salary` | `salary_bucket` |
|---:|---:|---:|
| 10 | 3000 | 4 |
| 11 | 3500 | 4 |
| 12 | 4200 | 5 |
| 13 | 4200 | 5 |

Powers of two form logarithmic payment bins:

```sql
SELECT
  POWER(2, FLOOR(LN(amount) / LN(2))) AS lower_bound,
  payment_outcome,
  COUNT(*) AS payment_count
FROM
  payments
WHERE
  amount > 0
GROUP BY
  POWER(2, FLOOR(LN(amount) / LN(2))),
  payment_outcome
ORDER BY
  lower_bound,
  payment_outcome;
```

Representative result:

| `lower_bound` | `payment_outcome` | `payment_count` |
|---:|---|---:|
| 32 | Approved | 2 |
| 64 | Approved | 3 |
| 64 | Declined | 1 |

Trino's `numeric_histogram` returns an approximate map of bucket centers to weights.
Its map expansion appears in the semi-structured data section.

## Subqueries and common table expressions

Scalar, correlated, `EXISTS`, and `IN` subqueries are introduced under
[predicates](#filter-rows-with-predicates). This section focuses on naming and chaining
query blocks.

A common table expression (CTE) names a query for the duration of one statement. A
CTE improves decomposition and can be referenced by later CTEs. It does not promise
materialization; the optimizer may inline or materialize it.

```sql
WITH
  customer_totals AS (
    SELECT
      customer_id,
      SUM(amount) AS total_amount
    FROM
      orders
    GROUP BY
      customer_id
  ),
  above_average AS (
    SELECT
      customer_id,
      total_amount
    FROM
      customer_totals
    WHERE
      total_amount > (
        SELECT
          AVG(total_amount)
        FROM
          customer_totals
      )
  )
SELECT
  c.first_name,
  a.total_amount
FROM
  above_average AS a
JOIN
  customers AS c
  ON c.id = a.customer_id
ORDER BY
  a.total_amount DESC;
```

| `first_name` | `total_amount` |
|---|---:|
| Ava | 200.00 |

## Window functions

A window function calculates across a set of rows while retaining one output row for
each input row. Window functions run after `WHERE`, grouping, and `HAVING`, but before
the final `ORDER BY` and `LIMIT`.

```text
rows left after filtering and grouping
                 |
                 v
PARTITION BY divides independent windows
                 |
                 v
ORDER BY establishes sequence and peer groups
                 |
                 v
frame selects rows relative to the current row
                 |
                 v
window function returns one value for the current row
```

General form:

```sql
window_function(arguments) [IGNORE NULLS]
OVER (
  PARTITION BY partition_expression
  ORDER BY ordering_expression
  ROWS BETWEEN frame_start AND frame_end
)
```

`IGNORE NULLS` placement and support vary by engine.

### Window-function families

| Family | Examples | Purpose |
|---|---|---|
| Aggregate | `SUM`, `AVG`, `COUNT` | Aggregate a window frame while retaining detail rows. |
| Ranking | `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE`, `PERCENT_RANK` | Number or rank rows within a partition. |
| Value | `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `LEAD`, `LAG` | Read a value at a position relative to the current row. |

For a partition with `n` rows and a row rank `r`, `PERCENT_RANK` is
`(r - 1) / (n - 1)` when `n > 1`.

```sql
SELECT
  department_id,
  last_name,
  salary,
  ROW_NUMBER() OVER (
    PARTITION BY department_id
    ORDER BY salary DESC, last_name
  ) AS row_number,
  RANK() OVER (
    PARTITION BY department_id
    ORDER BY salary DESC
  ) AS salary_rank,
  DENSE_RANK() OVER (
    PARTITION BY department_id
    ORDER BY salary DESC
  ) AS dense_salary_rank
FROM
  employees
ORDER BY
  department_id,
  row_number;
```

| `department_id` | `last_name` | `salary` | `row_number` | `salary_rank` | `dense_salary_rank` |
|---:|---|---:|---:|---:|---:|
| 100 | Chen | 3500 | 1 | 1 | 1 |
| 100 | Bell | 3000 | 2 | 2 | 2 |
| 200 | Dube | 4200 | 1 | 1 | 1 |
| 200 | Evans | 4200 | 2 | 1 | 1 |

`ROW_NUMBER` is always consecutive. `RANK` gives tied rows the same placement and
leaves a gap after the tie. `DENSE_RANK` gives ties the same placement without gaps.
`NTILE(n)` distributes ordered rows among `n` numbered buckets.

### Value functions

```sql
SELECT
  department_id,
  last_name,
  salary,
  FIRST_VALUE(salary) OVER (
    PARTITION BY department_id
    ORDER BY salary DESC
  ) AS department_high_salary,
  LAG(salary) OVER (
    PARTITION BY department_id
    ORDER BY salary DESC, last_name
  ) AS prior_row_salary,
  LEAD(salary) OVER (
    PARTITION BY department_id
    ORDER BY salary DESC, last_name
  ) AS next_row_salary
FROM
  employees
ORDER BY
  department_id,
  salary DESC,
  last_name;
```

| `department_id` | `last_name` | `salary` | `department_high_salary` | `prior_row_salary` | `next_row_salary` |
|---:|---|---:|---:|---:|---:|
| 100 | Chen | 3500 | 3500 | `NULL` | 3000 |
| 100 | Bell | 3000 | 3500 | 3500 | `NULL` |
| 200 | Dube | 4200 | 4200 | `NULL` | 4200 |
| 200 | Evans | 4200 | 4200 | 4200 | `NULL` |

`FIRST_VALUE` and `LAST_VALUE` operate on the current frame, not automatically on
the whole partition. Frame choice is therefore part of their meaning.

### `ROWS` and `RANGE` frames

`ROWS` counts physical row positions in the window ordering. `RANGE` groups peers and
uses ordering values; an interval-based range can express a time window.

```sql
SELECT
  bank_account_id,
  activity_id,
  effective_date,
  balance,
  SUM(balance) OVER (
    PARTITION BY bank_account_id
    ORDER BY effective_date, activity_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS row_running_balance,
  SUM(balance) OVER (
    PARTITION BY bank_account_id
    ORDER BY effective_date
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS date_running_balance
FROM
  account_activity
ORDER BY
  bank_account_id,
  effective_date,
  activity_id;
```

Representative result with two rows on the same date:

| `bank_account_id` | `activity_id` | `effective_date` | `balance` | `row_running_balance` | `date_running_balance` |
|---:|---:|---|---:|---:|---:|
| 1 | 1 | 2026-02-01 | 10 | 10 | 30 |
| 1 | 2 | 2026-02-01 | 20 | 30 | 30 |
| 1 | 3 | 2026-02-02 | 5 | 35 | 35 |

An interval-based seven-day moving average requires engine support for interval
`RANGE` frames:

```sql
SELECT
  bank_account_id,
  effective_date,
  AVG(balance) OVER (
    PARTITION BY bank_account_id
    ORDER BY effective_date
    RANGE BETWEEN INTERVAL '6' DAY PRECEDING AND CURRENT ROW
  ) AS seven_day_average
FROM
  daily_balances
ORDER BY
  bank_account_id,
  effective_date;
```

Representative result:

| `bank_account_id` | `effective_date` | `seven_day_average` |
|---:|---|---:|
| 1 | 2026-02-01 | 100.00 |
| 1 | 2026-02-02 | 110.00 |

To make `LAST_VALUE` see the entire partition, end the frame at `UNBOUNDED FOLLOWING`:

```sql
SELECT
  bank_account_id,
  effective_date,
  LAST_VALUE(effective_date) OVER (
    PARTITION BY bank_account_id
    ORDER BY effective_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS partition_max_date,
  LAST_VALUE(effective_date) OVER (
    PARTITION BY bank_account_id
    ORDER BY effective_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS max_date_as_of_row
FROM
  daily_balances
ORDER BY
  bank_account_id,
  effective_date;
```

Representative result:

| `bank_account_id` | `effective_date` | `partition_max_date` | `max_date_as_of_row` |
|---:|---|---|---|
| 1 | 2026-02-01 | 2026-02-03 | 2026-02-01 |
| 1 | 2026-02-02 | 2026-02-03 | 2026-02-02 |
| 1 | 2026-02-03 | 2026-02-03 | 2026-02-03 |

Without `ORDER BY`, the entire partition is normally the frame. With `ORDER BY`, do
not rely on an implicit default when peer handling matters; write the frame.

### Carry the last non-null value forward

On Trino and other engines that support null treatment for value functions,
`LAST_VALUE ... IGNORE NULLS` fills a sparse change log through the current row.

```sql
SELECT
  organization_id,
  created_date,
  LAST_VALUE(ELEMENT_AT(current_values, 'isActive')) IGNORE NULLS OVER (
    PARTITION BY organization_id
    ORDER BY created_date, event_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS is_active_as_of_row
FROM
  organization_changes
ORDER BY
  organization_id,
  created_date;
```

Representative result:

| `organization_id` | `created_date` | `is_active_as_of_row` |
|---:|---|---|
| 8 | 2026-01-01 | true |
| 8 | 2026-01-02 | true |
| 8 | 2026-01-03 | false |

The original `CASE` form is equivalent but redundant when the value function already
includes the current row and ignores nulls:

```sql
SELECT
  CASE
    WHEN ELEMENT_AT(current_values, 'isActive') IS NOT NULL
      THEN ELEMENT_AT(current_values, 'isActive')
    ELSE LAST_VALUE(ELEMENT_AT(current_values, 'isActive')) IGNORE NULLS OVER (
      PARTITION BY organization_id
      ORDER BY created_date, event_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
  END AS is_active_as_of_row
FROM
  organization_changes;
```

Representative result:

| `is_active_as_of_row` |
|---|
| true |
| true |
| false |

### Named windows

The `WINDOW` clause names a specification that several expressions can reuse.

```sql
SELECT
  order_key,
  clerk,
  total_price,
  RANK() OVER clerk_window AS price_rank,
  COUNT(*) OVER (
    clerk_window
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS rows_through_current_price
FROM
  orders_by_clerk
WINDOW clerk_window AS (
  PARTITION BY clerk
  ORDER BY total_price DESC
)
ORDER BY
  clerk,
  price_rank;
```

Representative result:

| `order_key` | `clerk` | `total_price` | `price_rank` | `rows_through_current_price` |
|---:|---|---:|---:|---:|
| 1 | Clerk 1 | 200.00 | 1 | 1 |
| 2 | Clerk 1 | 150.00 | 2 | 2 |

### Filter window results with `QUALIFY`

Snowflake, Amazon Redshift, BigQuery, and some other engines support `QUALIFY`, which
filters after window functions are evaluated.

```sql
SELECT
  salesperson,
  sale_date,
  amount
FROM
  sales
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY salesperson
    ORDER BY amount DESC, sale_date, sale_id
  ) = 1
ORDER BY
  salesperson;
```

Representative result:

| `salesperson` | `sale_date` | `amount` |
|---|---|---:|
| Ana | 2026-02-03 | 900.00 |
| Bo | 2026-02-02 | 750.00 |

The portable shape uses a subquery:

```sql
SELECT
  salesperson,
  sale_date,
  amount
FROM
  (
    SELECT
      salesperson,
      sale_date,
      amount,
      ROW_NUMBER() OVER (
        PARTITION BY salesperson
        ORDER BY amount DESC, sale_date, sale_id
      ) AS row_number
    FROM
      sales
  ) AS ranked_sales
WHERE
  row_number = 1
ORDER BY
  salesperson;
```

Representative result:

| `salesperson` | `sale_date` | `amount` |
|---|---|---:|
| Ana | 2026-02-03 | 900.00 |
| Bo | 2026-02-02 | 750.00 |

## String functions and cleanup

String functions are among the least portable parts of otherwise simple SQL. Check
the active engine's rules for nulls, regular-expression syntax, Unicode, and indexing.

### Concatenate values

`CONCAT` is widely available. The standard concatenation operator is `||`, but some
engines configure or interpret it differently.

```sql
SELECT
  CONCAT(first_name, ', ', city) AS customer_location,
  first_name || ', ' || city AS operator_location
FROM
  customers
WHERE
  id = 1;
```

| `customer_location` | `operator_location` |
|---|---|
| Ava, New York | Ava, New York |

`CONCAT_WS` means concatenate with separator. In engines such as Trino and
PostgreSQL, it skips null arguments; a null separator does not produce a useful
result.

```sql
SELECT
  CONCAT('@', 'HASH', 'example.com') AS plain_concat,
  CONCAT_WS('@', 'HASH', 'example.com') AS with_separator;
```

| `plain_concat` | `with_separator` |
|---|---|
| @HASHexample.com | HASH@example.com |

### Case, length, and trimming

```sql
SELECT
  first_name,
  UPPER(last_name) AS upper_last_name,
  LOWER(city) AS lower_city,
  LENGTH(last_name) AS last_name_length,
  TRIM('  SQL  ') AS trimmed_text
FROM
  customers
WHERE
  id = 1;
```

| `first_name` | `upper_last_name` | `lower_city` | `last_name_length` | `trimmed_text` |
|---|---|---|---:|---|
| Ava | COLE | new york | 4 | SQL |

Common functions include:

| Expression | Purpose |
|---|---|
| `SUBSTR(string, start, length)` | Return part of a string. Index rules vary by engine. |
| `LENGTH(string)` | Return string length, usually in characters. Byte-length functions are separate. |
| `TRIM(string)` | Remove leading and trailing pad characters, normally spaces. |
| `LTRIM(string)` | Remove leading pad characters. |
| `RTRIM(string)` | Remove trailing pad characters. |
| `UPPER(string)` | Convert letters to uppercase using engine/collation rules. |
| `LOWER(string)` | Convert letters to lowercase using engine/collation rules. |

### Replace text

`REPLACE` substitutes literal substrings.

```sql
SELECT
  REPLACE('database', 'a', '--') AS changed_text,
  REPLACE('New York', ' ', '') AS no_literal_spaces;
```

| `changed_text` | `no_literal_spaces` |
|---|---|
| d--t--b--se | NewYork |

Removing `' '` affects ordinary space characters only. Use an engine-specific
whitespace regular expression when tabs and line breaks should also be removed.

### Normalize a name

This Trino-style expression trims the ends, removes selected punctuation, and
lowercases the remaining value:

```sql
SELECT
  LOWER(
    REGEXP_REPLACE(
      TRIM(name),
      '[.,/#!$%^&*;:{}=_`~()-]',
      ''
    )
  ) AS normalized_name
FROM
  vendor_names;
```

Representative result:

| `normalized_name` |
|---|
| northwind market |

Trino supports a lambda replacement that can title-case simple Latin-letter word segments:

```sql
SELECT
  REGEXP_REPLACE(
    'aLiCe smITH',
    '(\w)(\w*)',
    match -> UPPER(match[1]) || LOWER(match[2])
  ) AS proper_name;
```

| `proper_name` |
|---|
| Alice Smith |

This is not a complete human-name algorithm. `\w`, word boundaries, particles, and
case conversion are engine- and locale-dependent.

### Extract an email domain

When the input contract guarantees one `@`, `SPLIT_PART` is clearer than removing a
prefix with a regular expression.

```sql
SELECT
  LOWER(SPLIT_PART(email, '@', 2)) AS email_domain
FROM
  contacts
WHERE
  email = 'User@Example.com';
```

| `email_domain` |
|---|
| example.com |

The original regular-expression approach remains useful when the engine lacks
`SPLIT_PART`, but it leaves malformed addresses without `@` unchanged:

```sql
SELECT
  LOWER(REGEXP_REPLACE(email, '^(.*?)@', '')) AS email_domain
FROM
  contacts
WHERE
  email = 'User@Example.com';
```

| `email_domain` |
|---|
| example.com |

### Normalize a United States phone number

This Trino-style example accepts a leading `+1`, then removes non-digits. It does not
validate length or extensions, so validation should follow normalization.

```sql
SELECT
  REGEXP_REPLACE(
    REGEXP_REPLACE('+1 (312) 555-0199', '^\+1', ''),
    '[^0-9]',
    ''
  ) AS national_number;
```

| `national_number` |
|---|
| 3125550199 |

### Split a delimited value

`SPLIT_PART(string, delimiter, position)` commonly uses one-based positions.

```sql
SELECT
  SPLIT_PART('user@example.com', '@', 2) AS domain;
```

| `domain` |
|---|
| example.com |

### Repeat text

```sql
SELECT
  REPEAT('0', 20) AS twenty_zeroes,
  id
FROM
  identifiers
WHERE
  id <> REPEAT('0', 20);
```

Representative result:

| `twenty_zeroes` | `id` |
|---|---|
| 00000000000000000000 | 00000000000000000017 |

### Phonetic matching with `SOUNDEX`

`SOUNDEX` converts an English word or name to an implementation-specific phonetic
code. It can surface spelling variants, but it is lossy, English-centric, and not a
general entity-resolution method. The example below uses MySQL. PostgreSQL provides
`SOUNDEX` through the `fuzzystrmatch` extension; Trino does not provide it as a core
function.

```sql
SELECT
  SOUNDEX('Robert') AS robert_code,
  SOUNDEX('Rupert') AS rupert_code;
```

Typical result on implementations using the common four-character Soundex form:

| `robert_code` | `rupert_code` |
|---|---|
| R163 | R163 |

The usual algorithm retains the first letter, maps groups of similar consonants to
digits, suppresses eligible adjacent duplicates, and pads or truncates the result.
Details around vowels, `H`, `W`, non-ASCII characters, and nulls vary by engine.

## Dates, timestamps, and time zones

A date is a calendar day. A time is a wall-clock time. A timestamp may or may not
carry time-zone or offset information, depending on its type and engine. Unix time is
different: it counts elapsed units from `1970-01-01 00:00:00` Coordinated Universal
Time (UTC).

### Truncate a date or timestamp

`DATE_TRUNC` returns the beginning of a named unit. This Trino/PostgreSQL-style
example casts the result to a date so the result type is consistent.

```sql
SELECT
  CAST(
    DATE_TRUNC('month', TIMESTAMP '2026-02-19 00:00:00')
    AS DATE
  ) AS month_start;
```

| `month_start` |
|---|
| 2026-02-01 |

### Add and compare dates

Trino uses `date_add(unit, value, input)` and `date_diff(unit, start, end)`:

```sql
SELECT
  DATE_ADD('year', -1, DATE '2026-02-19') AS one_year_earlier,
  DATE_DIFF('day', DATE '2026-02-01', DATE '2026-02-19') AS elapsed_days;
```

| `one_year_earlier` | `elapsed_days` |
|---|---:|
| 2025-02-19 | 18 |

Snowflake and Redshift use `DATEADD`/`DATEDIFF` forms whose accepted arguments and
boundary behavior differ. Label the engine rather than mechanically changing the
function's punctuation.

### Extract a date part

```sql
SELECT
  EXTRACT(YEAR FROM TIMESTAMP '2026-02-19 14:30:00') AS calendar_year;
```

| `calendar_year` |
|---:|
| 2026 |

### Last day of a period

Snowflake, Redshift, MySQL, and other engines provide `LAST_DAY` with different
optional arguments.

```sql
SELECT
  LAST_DAY(DATE '2026-02-10') AS month_end;
```

| `month_end` |
|---|
| 2026-02-28 |

### Day name and formatted text

Trino's MySQL-compatible `DATE_FORMAT` uses percent directives:

```sql
SELECT
  DATE_FORMAT(
    DATE_TRUNC('day', CAST(TIMESTAMP '2026-02-19 14:30:00' AS TIMESTAMP)),
    '%W'
  ) AS weekday_name;
```

| `weekday_name` |
|---|
| Thursday |

Formatting creates text. It does not preserve a timestamp's type or time-zone
semantics.

### Unix timestamps

Unix timestamps are normally seconds or milliseconds since the Unix epoch in UTC.
Contemporary second values often have 10 digits and millisecond values 13, but digit
length is not a durable type contract.

MySQL and Trino provide functions named `FROM_UNIXTIME`, with different return types
and time-zone behavior:

Assuming the session time zone is UTC:

```sql
SELECT
  FROM_UNIXTIME(1255033470) AS decoded_timestamp;
```

In UTC, the instant is:

| `decoded_timestamp` |
|---|
| 2009-10-08 20:24:30 UTC |

A session-zone rendering can show a different wall-clock value while representing
the same instant.

For ISO 8601 metadata in Trino, parse the encoded offset before changing display
zone:

```sql
SELECT
  FROM_ISO8601_TIMESTAMP('2024-07-04T13:46:08Z')
    AT TIME ZONE 'America/Chicago' AS central_time;
```

| `central_time` |
|---|
| 2024-07-04 08:46:08 America/Chicago |

If a JSON field stores milliseconds as a number, divide by `1000.0` rather than
assuming the first ten characters are seconds:

```sql
SELECT
  FROM_UNIXTIME(
    CAST(JSON_EXTRACT_SCALAR(record_metadata, '$.CreateTime') AS DOUBLE) / 1000.0
  ) AS created_at
FROM
  ingestion_records;
```

Representative result:

| `created_at` |
|---|
| 2024-04-16 03:56:18 UTC |

### Convert a time zone

Snowflake and Redshift provide `CONVERT_TIMEZONE`; argument forms depend on the input
timestamp type.

```sql
SELECT
  CONVERT_TIMEZONE(
    'UTC',
    'America/New_York',
    TIMESTAMP '2026-02-19 18:00:00'
  ) AS new_york_time;
```

| `new_york_time` |
|---|
| 2026-02-19 13:00:00 |

`AT TIME ZONE` behaves differently for zoned and unzoned timestamp inputs. State the
source zone and type explicitly. Casting a zoned timestamp to text, taking a substring,
and casting it back loses zone information and precision; it does not lock an instant.

The original Trino workaround below should therefore be treated as formatting followed
by information loss, not as a safe instant-preserving conversion:

```sql
SELECT
  DATE_FORMAT(
    created_date AT TIME ZONE 'America/New_York',
    '%Y-%m-%d %H:%i:%s.%f'
  ) AS eastern_time_text,
  CAST(
    SUBSTR(
      CAST(created_date AT TIME ZONE 'America/New_York' AS VARCHAR),
      1,
      23
    ) AS TIMESTAMP
  ) AS eastern_wall_clock_without_zone
FROM
  events;
```

Representative result:

| `eastern_time_text` | `eastern_wall_clock_without_zone` |
|---|---|
| 2026-02-19 13:00:00.000000 | 2026-02-19 13:00:00.000 |

The first column is text. The second is an unzoned wall-clock timestamp and can no
longer identify the original instant by itself.

### ISO 8601

`2024-07-04T13:46:08Z` uses `T` between date and time and `Z` for UTC. An explicit
offset such as `2024-07-04T13:46:08+02:00` identifies another offset from UTC.

### Current local timestamp

```sql
SELECT
  LOCALTIMESTAMP AS local_timestamp;
```

Representative result shape:

| `local_timestamp` |
|---|
| 2026-02-19 14:30:00.000 |

The evaluation instant and session time zone are engine-specific. Store instants with
an unambiguous zone or offset when they must survive movement between systems.

### Generate a date sequence in Trino

`sequence` returns an array. `UNNEST` expands it into rows.

```sql
SELECT
  date_value
FROM
  UNNEST(
    SEQUENCE(
      DATE '2026-02-01',
      DATE '2026-02-03',
      INTERVAL '1' DAY
    )
  ) AS generated_dates (date_value)
ORDER BY
  date_value;
```

| `date_value` |
|---|
| 2026-02-01 |
| 2026-02-02 |
| 2026-02-03 |

## Arrays, maps, JSON, and complex types

Semi-structured functions are dialect-specific. Distinguish a SQL array or map from
JavaScript Object Notation (JSON) text and from an engine-native semi-structured type.

### Aggregate non-null values into an array

This Trino example filters before aggregation and sorts the resulting array instead
of adding a null and removing it later.

```sql
SELECT
  ARRAY_SORT(
    ARRAY_AGG(DISTINCT category)
      FILTER (WHERE category IS NOT NULL)
  ) AS categories
FROM
  events;
```

Representative result:

| `categories` |
|---|
| `[billing, login]` |

The original two-step form is valid in PostgreSQL, where `ARRAY_REMOVE` compares
elements with null-safe semantics:

```sql
SELECT
  ARRAY_REMOVE(
    ARRAY_AGG(DISTINCT category ORDER BY category),
    NULL
  ) AS categories
FROM
  events;
```

Representative result:

| `categories` |
|---|
| `[billing, login]` |

### Read JSON in Trino

`JSON_EXTRACT` returns JSON. Use `JSON_EXTRACT_SCALAR` when the selected value should
become a SQL scalar. Array index `1` addresses the second element.

```sql
SELECT
  CASE
    WHEN TRY(JSON_ARRAY_LENGTH(json_column)) > 1
      THEN TRY(JSON_EXTRACT_SCALAR(json_column, '$[1].value'))
    ELSE NULL
  END AS second_value
FROM
  source_events;
```

Representative result:

| `second_value` |
|---|
| approved |

Use one identifier consistently. A reference such as `t.json.Column` means a
different nested identifier than `t.json_column` and was not valid in the original
example.

Snowflake uses colon and bracket traversal on `VARIANT`; Redshift provides functions
such as `JSON_EXTRACT_PATH_TEXT`. Those expressions are not drop-in Trino syntax.

### Forward-fill a JSON field in legacy Redshift text JSON

The original note called this `element_at`, but it uses Redshift's
`JSON_EXTRACT_PATH_TEXT`, not Trino's `ELEMENT_AT`. The following preserves the
original change-log approach and corrects its structure:

```sql
WITH
  daily_changes AS (
    SELECT
      DATE_TRUNC('day', created_date) AS created_date,
      organization_id,
      '{'
        || LISTAGG(
             '"' || field_name || '": ' || current_value,
             ', '
           ) WITHIN GROUP (ORDER BY field_name)
        || '}' AS current_values_text
    FROM
      organization_change_fields
    GROUP BY
      DATE_TRUNC('day', created_date),
      organization_id
  )
SELECT
  created_date,
  organization_id,
  LAST_VALUE(
    JSON_EXTRACT_PATH_TEXT(current_values_text, 'isActive')
  ) IGNORE NULLS OVER (
    PARTITION BY organization_id
    ORDER BY created_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS is_active_as_of_row
FROM
  daily_changes
ORDER BY
  organization_id,
  created_date;
```

Representative result:

| `created_date` | `organization_id` | `is_active_as_of_row` |
|---|---:|---|
| 2026-01-01 | 8 | true |
| 2026-01-02 | 8 | true |
| 2026-01-03 | 8 | false |

The query is fragile because string concatenation does not safely escape JSON keys or
values. On current Redshift designs, parse source JSON into `SUPER` and use native
semi-structured construction and navigation rather than assembling JSON text with
`LISTAGG`.

### Access a map element in Trino

`ELEMENT_AT(map_value, key)` returns the value for a key or `NULL` when the key is
absent.

```sql
SELECT
  ELEMENT_AT(
    MAP(ARRAY['isActive', 'tier'], ARRAY['true', 'gold']),
    'isActive'
  ) AS is_active;
```

| `is_active` |
|---|
| true |

### Expand a Trino histogram map

`NUMERIC_HISTOGRAM` returns an approximate map whose keys are bucket centers and whose
values are weights. It does not return exact ranges or exact counts.

```sql
WITH
  histogram AS (
    SELECT
      NUMERIC_HISTOGRAM(10, settled_amount) AS buckets
    FROM
      card_transactions
    WHERE
      status = 4
      AND is_reissued = '0'
      AND settlement_date IS NOT NULL
      AND transaction_date >= DATE '2022-09-01'
  )
SELECT
  bucket,
  weight
FROM
  histogram
CROSS JOIN UNNEST(buckets) AS expanded (bucket, weight)
ORDER BY
  bucket;
```

Representative result:

| `bucket` | `weight` |
|---:|---:|
| 25.4 | 18.0 |
| 71.8 | 31.0 |

### Do not build JSON with string aggregation

This original pattern can produce invalid JSON because keys and values are not escaped,
null handling is ambiguous, and embedded quotes break the document:

```sql
SELECT
  '{'
    || LISTAGG('"' || field_name || '": ' || current_value, ', ')
         WITHIN GROUP (ORDER BY field_name)
    || '}' AS unsafe_json
FROM
  change_fields
GROUP BY
  organization_id,
  created_date;
```

Representative output that only appears valid for simple inputs:

| `unsafe_json` |
|---|
| `{"isActive": true, "tier": "gold"}` |

Use the engine's object or map aggregation and JSON serializer instead. For example,
Snowflake provides `OBJECT_AGG`; Trino can aggregate a map and format it as JSON.

### Trino `ROW`

`ROW` is a typed structure, comparable to a record or struct.

```sql
SELECT
  CAST(
    ROW('John', 'Doe', 42)
    AS ROW(first_name VARCHAR, last_name VARCHAR, age INTEGER)
  ) AS person;
```

| `person` |
|---|
| `{first_name=John, last_name=Doe, age=42}` |

A table can nest rows:

```sql
CREATE TABLE people (
  id BIGINT,
  info ROW(
    name VARCHAR,
    age INTEGER,
    location ROW(city VARCHAR, state VARCHAR)
  )
);
```

After rows are loaded, fields are addressed with dot notation:

```sql
SELECT
  id,
  info.name,
  info.age,
  info.location.city
FROM
  people;
```

Representative result:

| `id` | `name` | `age` | `city` |
|---:|---|---:|---|
| 1 | Alice | 30 | Chicago |

### Binary values

`VARBINARY` stores variable-length bytes. Client display and literal syntax vary.
Trino accepts hexadecimal binary literals using `X'...'`.

```sql
SELECT
  X'89504E470D0A1A0A' AS png_signature;
```

Representative hexadecimal display:

| `png_signature` |
|---|
| `89504E470D0A1A0A` |

### Snowflake `VARIANT` and `OBJECT`

`VARIANT` stores parsed semi-structured values. JSON, Avro, Parquet, ORC, and XML are
source formats that can be parsed or loaded; the cell does not literally become one
of those file formats. Size limits are implementation details, so do not preserve an
old fixed `16 MB` assumption in new designs.

```sql
CREATE TABLE profile_events (
  id INTEGER,
  raw_data VARIANT
);

INSERT INTO profile_events (id, raw_data)
SELECT
  1,
  PARSE_JSON('{"name":"Alice","orders":[1001,1002]}');
```

```sql
SELECT
  raw_data:name::VARCHAR AS name,
  raw_data:orders[0]::INTEGER AS first_order
FROM
  profile_events
WHERE
  id = 1;
```

| `name` | `first_order` |
|---|---:|
| Alice | 1001 |

`OBJECT_CONSTRUCT` returns a Snowflake `OBJECT`, which can be held in a `VARIANT`.

```sql
SELECT
  OBJECT_CONSTRUCT(
    'name', 'Alice',
    'age', 30,
    'active', TRUE
  ) AS user_info;
```

| `user_info` |
|---|
| `{"active":true,"age":30,"name":"Alice"}` |

Key order is not semantically meaningful. SQL null-valued pairs and JSON `null` are
not the same; choose `OBJECT_CONSTRUCT_KEEP_NULL` when SQL null-valued keys must remain.

## Change rows with DML

The row-changing DML statements in this section are `INSERT`, `UPDATE`, `DELETE`, and
`MERGE`. Run destructive statements in an appropriate transaction or test environment,
preview the predicate with `SELECT`, and verify the resulting state.

### Insert rows

Prefer an explicit target column list. It survives column-order changes and makes
omitted defaults visible.

Before:

| `region_id` | `region_name` | `country` |
|---:|---|---|
| 2 | West | USA |

```sql
INSERT INTO company_regions (
  region_id,
  region_name,
  country
)
VALUES
  (1, 'Northeast', 'USA');
```

After:

```sql
SELECT
  region_id,
  region_name,
  country
FROM
  company_regions
ORDER BY
  region_id;
```

| `region_id` | `region_name` | `country` |
|---:|---|---|
| 1 | Northeast | USA |
| 2 | West | USA |

`INSERT INTO table_name DEFAULT VALUES` inserts one row using column defaults. A
column without a default becomes `NULL` only when it is nullable; otherwise the
statement fails.

```sql
INSERT INTO audit_log DEFAULT VALUES;
```

If `created_at` defaults to the current timestamp and `message` is nullable:

```sql
SELECT
  created_at,
  message
FROM
  audit_log
ORDER BY
  created_at DESC
LIMIT 1;
```

Representative result:

| `created_at` | `message` |
|---|---|
| 2026-02-19 14:30:00 | `NULL` |

### Construct rows with `VALUES`

`VALUES` can be used as a standalone row source or derived table.

```sql
SELECT
  id,
  name
FROM
  (
    VALUES
      (1, 'a'),
      (2, 'b'),
      (3, 'c')
  ) AS rows_to_use (id, name)
ORDER BY
  id;
```

| `id` | `name` |
|---:|---|
| 1 | a |
| 2 | b |
| 3 | c |

### Update rows

`UPDATE` changes every row for which its predicate is true. Omitting `WHERE` updates
the entire table.

Before:

| `employee_id` | `location` |
|---:|---|
| 123 | usa |
| 124 | can |

```sql
UPDATE workday_snapshot
SET
  location = 'mex'
WHERE
  employee_id = 123;
```

After:

```sql
SELECT
  employee_id,
  location
FROM
  workday_snapshot
ORDER BY
  employee_id;
```

| `employee_id` | `location` |
|---:|---|
| 123 | mex |
| 124 | can |

A scalar aggregate can impute missing values. Calculate the average from the pre-update
state; engine rules determine whether the target table can also appear in the subquery.

Before:

| `automobile_id` | `height` |
|---:|---:|
| 1 | 60.0 |
| 2 | 64.0 |
| 3 | `NULL` |

The average of the two present values is `62.0`.

```sql
UPDATE automobiles
SET
  height = (
    SELECT
      AVG(height)
    FROM
      automobiles
  )
WHERE
  height IS NULL;
```

```sql
SELECT
  automobile_id,
  height
FROM
  automobiles
ORDER BY
  automobile_id;
```

Representative result after replacing one missing value with the prior average:

| `automobile_id` | `height` |
|---:|---:|
| 1 | 60.0 |
| 2 | 64.0 |
| 3 | 62.0 |

### Delete rows

`DELETE` removes qualifying rows while retaining the table. Without `WHERE`, it
removes every row.

Before:

| `id` | `first_name` |
|---:|---|
| 1 | Ana |
| 2 | Bo |

```sql
DELETE FROM employees_to_remove
WHERE
  id = 1;
```

After:

```sql
SELECT
  id,
  first_name
FROM
  employees_to_remove
ORDER BY
  id;
```

| `id` | `first_name` |
|---:|---|
| 2 | Bo |

### Merge source changes into a target

`MERGE` applies clauses according to whether source and target rows match. It becomes
an upsert when matched rows are updated and unmatched source rows are inserted. Some
engines also support deletion clauses. Ensure the source has at most one applicable
row per target key when deterministic matching is required.

Target before:

| `customer_id` | `status` | `updated_at` |
|---:|---|---|
| 1 | active | 2026-02-01 |
| 2 | active | 2026-02-01 |

Source:

| `customer_id` | `status` | `updated_at` |
|---:|---|---|
| 2 | paused | 2026-02-19 |
| 3 | active | 2026-02-19 |

```sql
MERGE INTO customer_status AS target
USING customer_status_updates AS source
  ON target.customer_id = source.customer_id
WHEN MATCHED THEN
  UPDATE SET
    status = source.status,
    updated_at = source.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    status,
    updated_at
  )
  VALUES (
    source.customer_id,
    source.status,
    source.updated_at
  );
```

Target after:

```sql
SELECT
  customer_id,
  status,
  updated_at
FROM
  customer_status
ORDER BY
  customer_id;
```

| `customer_id` | `status` | `updated_at` |
|---:|---|---|
| 1 | active | 2026-02-01 |
| 2 | paused | 2026-02-19 |
| 3 | active | 2026-02-19 |

## Define tables and types

Data Definition Language (DDL) creates, changes, and removes database objects. DDL
transaction and recovery behavior varies, so understand the engine before running it
against important objects.

### Create a table

```sql
CREATE TABLE users (
  user_id INTEGER,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  city VARCHAR(100),
  PRIMARY KEY (user_id)
);
```

The table exists after the statement but contains no rows:

```sql
SELECT
  user_id,
  first_name,
  last_name,
  city
FROM
  users;
```

| `user_id` | `first_name` | `last_name` | `city` |
|---:|---|---|---|

### Common type families

| Family | Common types | Notes |
|---|---|---|
| Exact numeric | `SMALLINT`, `INTEGER`, `BIGINT`, `DECIMAL(p, s)` | Integers and fixed-precision decimals. Signedness and maximum precision vary. |
| Approximate numeric | `REAL`, `DOUBLE PRECISION`, `FLOAT` | Binary floating-point values; unsuitable for exact currency arithmetic. |
| Character | `CHAR(n)`, `VARCHAR(n)`, `VARCHAR`, `TEXT` | `CHAR` is fixed length; `VARCHAR` is variable length. Length units and limits vary. |
| Binary | `BINARY`, `VARBINARY`, `BLOB` | Raw bytes. Names, literal syntax, and maximum sizes vary. |
| Boolean | `BOOLEAN` | `TRUE`, `FALSE`, and possibly `NULL`. Some engines use numeric substitutes. |
| Date/time | `DATE`, `TIME`, `TIMESTAMP`, timestamp-with-zone variants | A SQL timestamp is not a Unix timestamp. Zone semantics vary by type and engine. |
| Semi-structured | `JSON`, Trino `ROW`/`ARRAY`/`MAP`, Snowflake `VARIANT`/`OBJECT`/`ARRAY` | Engine-specific storage and access syntax. |

MySQL forms such as `FLOAT(M,D)`, `DOUBLE(M,D)`, `DATETIME`, unsigned integers, and
`AUTO_INCREMENT` should not define a generic SQL type reference. Use exact
`DECIMAL(p, s)` for fixed decimal precision; use floating-point types only when
approximation is acceptable.

A three-part object name is engine-specific. This Snowflake-style example corrects
the missing comma in the original note:

```sql
CREATE TABLE prod.jaffle_shop.jaffles (
  id VARCHAR(255),
  jaffle_name VARCHAR(255),
  created_at TIMESTAMP,
  ingredients_list VARCHAR(255),
  is_active BOOLEAN
);
```

PostgreSQL's `tsvector` is a full-text-search document representation, not a portable
string type. It stores normalized lexemes for indexed text search.

### Constraints and defaults

| Constraint | Effect |
|---|---|
| `NOT NULL` | Rejects a missing value in the constrained column. |
| `UNIQUE` | Requires the constrained key values to be unique. Treatment of multiple nulls and enforcement vary. |
| `PRIMARY KEY` | Declares the row identifier, conceptually unique and non-null. Enforcement and index creation are engine-specific. |
| `CHECK` | Accepts rows whose expression is not false. Because `UNKNOWN` commonly passes, combine it with `NOT NULL` when null must be rejected. |
| `DEFAULT` | Supplies a value when the column is omitted or `DEFAULT` is requested. An explicit `NULL` remains null unless another rule intervenes. |
| `FOREIGN KEY` | Declares that values reference a candidate key in another relation. Enforcement varies by engine. |

```sql
CREATE TABLE accounts (
  account_id BIGINT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(320) UNIQUE,
  balance DECIMAL(18, 2) DEFAULT 0 NOT NULL,
  CHECK (balance >= 0)
);
```

MySQL uses `AUTO_INCREMENT` to generate numbers:

```sql
CREATE TABLE mysql_users (
  user_id INTEGER NOT NULL AUTO_INCREMENT,
  name VARCHAR(100) NOT NULL,
  PRIMARY KEY (user_id)
);
```

Generation does not promise gap-free values. The primary key, not `AUTO_INCREMENT`
alone, declares uniqueness. Other engines use identity columns, sequences, or their
own generation syntax.

## Change and remove objects

### Add, rename, change, and drop columns

Before:

| Column | Type |
|---|---|
| `person_id` | `INTEGER` |
| `first_name` | `CHAR(40)` |

```sql
ALTER TABLE people
ADD COLUMN date_of_birth DATE;

ALTER TABLE people
RENAME COLUMN first_name TO name;
```

PostgreSQL changes the character type with:

```sql
ALTER TABLE people
ALTER COLUMN name TYPE VARCHAR(100);
```

After:

| Column | Type |
|---|---|
| `person_id` | `INTEGER` |
| `name` | `VARCHAR(100)` |
| `date_of_birth` | `DATE` |

Drop a column only after checking dependencies and recovery options:

```sql
ALTER TABLE people
DROP COLUMN date_of_birth;
```

After:

| Column | Type |
|---|---|
| `person_id` | `INTEGER` |
| `name` | `VARCHAR(100)` |

### Rename a table

Portable PostgreSQL-style form:

```sql
ALTER TABLE people
RENAME TO users;
```

MySQL also supports:

```sql
RENAME TABLE people TO users;
```

After either operation, the object is addressed as `users`, not `people`:

```sql
SELECT
  person_id,
  name
FROM
  users;
```

Representative result:

| `person_id` | `name` |
|---:|---|
| 1 | Ana |

### `DELETE`, `TRUNCATE`, and `DROP`

| Statement | Scope | Object remains | Row filter | Typical category |
|---|---|---|---|---|
| `DELETE FROM payments WHERE ...` | Matching rows | Yes | Yes | DML |
| `DELETE FROM payments` | All rows | Yes | No predicate supplied | DML |
| `TRUNCATE TABLE payments` | All rows | Yes | No | DDL in common classifications |
| `DROP TABLE payments` | Table definition and its data | No | No | DDL |

`TRUNCATE` is commonly faster than row-by-row deletion, but transaction, identity,
privilege, and recovery behavior vary. `DROP` removes the object, so dependent views,
permissions, and metadata can also be affected.

Before truncation:

| Relation | Exists | Row count |
|---|---|---:|
| `payments` | Yes | 2 |

```sql
TRUNCATE TABLE payments;
```

After truncation:

```sql
SELECT
  COUNT(*) AS payment_count
FROM
  payments;
```

| `payment_count` |
|---:|
| 0 |

Before the drop, `payments` still exists with its definition, permissions, and zero
rows:

| Relation | Exists | Row count |
|---|---|---:|
| `payments` | Yes | 0 |

```sql
DROP TABLE payments;
```

After the drop:

| Relation | Exists | Row count |
|---|---|---|
| `payments` | No | Not applicable |

Querying `payments` now fails because the relation no longer exists.

## Views

A regular view stores a query definition. It normally reads underlying data when the
view is queried rather than storing a separate copy of every result row.

```sql
CREATE VIEW employee_salary_list AS
SELECT
  first_name,
  salary
FROM
  employees;
```

```sql
SELECT
  first_name,
  salary
FROM
  employee_salary_list
ORDER BY
  first_name;
```

| `first_name` | `salary` |
|---|---:|
| Ana | 3000 |
| Bo | 3500 |
| Cy | 4200 |
| Dee | 4200 |

`CREATE OR REPLACE VIEW` replaces the stored query definition; it does not update rows
through the view.

```sql
CREATE OR REPLACE VIEW employee_salary_list AS
SELECT
  first_name,
  salary
FROM
  employees
WHERE
  salary >= 4000;
```

```sql
SELECT
  first_name,
  salary
FROM
  employee_salary_list
ORDER BY
  first_name;
```

| `first_name` | `salary` |
|---|---:|
| Cy | 4200 |
| Dee | 4200 |

Replacement support, dependency rules, and whether a view is updatable vary by engine.

## Programmable objects

Functions return a value and can often appear inside a query. Procedures are invoked
with `CALL` and can perform multi-statement work. Languages, parameter modes,
transactions, and security models are engine-specific.

### PostgreSQL SQL function

```sql
CREATE OR REPLACE FUNCTION harmonic_mean(
  x NUMERIC,
  y NUMERIC
)
RETURNS NUMERIC
LANGUAGE SQL
IMMUTABLE
AS $$
  SELECT
    ROUND((2 * x * y) / NULLIF(x + y, 0), 2);
$$;
```

```sql
SELECT
  harmonic_mean(4, 12) AS result;
```

| `result` |
|---:|
| 6.00 |

`NULLIF` prevents division by zero when `x + y = 0`.

### PostgreSQL procedure with an `INOUT` parameter

```sql
CREATE OR REPLACE PROCEDURE increment_value(
  IN p_input_value INTEGER,
  INOUT p_output_value INTEGER
)
LANGUAGE plpgsql
AS $$
BEGIN
  p_output_value := p_input_value + 1;
END;
$$;
```

```sql
CALL increment_value(10, 0);
```

The procedure returns its `INOUT` value as a result row:

| `p_output_value` |
|---:|
| 11 |

A second procedure can call it and store the result:

```sql
CREATE OR REPLACE PROCEDURE increment_value_and_store(
  IN p_input_value INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_output_value INTEGER := 0;
BEGIN
  CALL increment_value(p_input_value, v_output_value);

  INSERT INTO temp_results (output_value)
  VALUES (v_output_value);
END;
$$;
```

```sql
CALL increment_value_and_store(10);
```

```sql
SELECT
  output_value
FROM
  temp_results;
```

| `output_value` |
|---:|
| 11 |

## Control access and transactions

### Grant and revoke privileges

`GRANT` adds privileges; `REVOKE` removes them. Use identifiers for users and roles,
not string literals, unless the engine's grammar explicitly requires otherwise.

State before the direct-grant changes:

| Principal | Object | Privilege | Directly granted |
|---|---|---|---|
| `someuser` | `my_table` | `SELECT` | No |
| `someuser` | `my_table` | `INSERT` | No |
| `someuser` | `my_table` | `DELETE` | No |
| `user1` | `employees` | `INSERT` | Yes |

```sql
GRANT
  SELECT,
  INSERT,
  DELETE
ON TABLE
  my_table
TO
  someuser;

REVOKE
  INSERT
ON TABLE
  employees
FROM
  user1;
```

State after the direct-grant changes:

| Principal | Object | Privilege | Directly granted |
|---|---|---|---|
| `someuser` | `my_table` | `SELECT` | Yes |
| `someuser` | `my_table` | `INSERT` | Yes |
| `someuser` | `my_table` | `DELETE` | Yes |
| `user1` | `employees` | `INSERT` | No |

In PostgreSQL, query the information schema to verify the direct table grants that
remain. A revoked direct grant is absent from the result.

```sql
SELECT
  grantee,
  table_name,
  privilege_type
FROM
  information_schema.role_table_grants
WHERE
  grantee IN ('someuser', 'user1')
  AND table_name IN ('my_table', 'employees')
ORDER BY
  grantee,
  table_name,
  privilege_type;
```

| `grantee` | `table_name` | `privilege_type` |
|---|---|---|
| `someuser` | `my_table` | `DELETE` |
| `someuser` | `my_table` | `INSERT` |
| `someuser` | `my_table` | `SELECT` |

A direct grant is not the same as effective access. A user can retain a privilege
through an inherited role, `PUBLIC`, table ownership, or superuser status. PostgreSQL's
`HAS_TABLE_PRIVILEGE` checks the effective privilege:

```sql
WITH privilege_checks (
  principal,
  table_name,
  privilege_type
) AS (
  VALUES
    ('someuser', 'public.my_table', 'SELECT'),
    ('someuser', 'public.my_table', 'INSERT'),
    ('someuser', 'public.my_table', 'DELETE'),
    ('user1', 'public.employees', 'INSERT')
)
SELECT
  principal,
  table_name,
  privilege_type,
  HAS_TABLE_PRIVILEGE(
    principal,
    table_name,
    privilege_type
  ) AS effective_access
FROM
  privilege_checks
ORDER BY
  principal,
  table_name,
  privilege_type;
```

Assuming neither principal has another path to these privileges:

| `principal` | `table_name` | `privilege_type` | `effective_access` |
|---|---|---|---|
| `someuser` | `public.my_table` | `DELETE` | `TRUE` |
| `someuser` | `public.my_table` | `INSERT` | `TRUE` |
| `someuser` | `public.my_table` | `SELECT` | `TRUE` |
| `user1` | `public.employees` | `INSERT` | `FALSE` |

PostgreSQL-style schema and existing-object grants:

```sql
GRANT USAGE
ON SCHEMA analytics
TO analyst_role;

GRANT SELECT
ON ALL TABLES IN SCHEMA analytics
TO analyst_role;
```

Existing-object grants do not normally cover objects created later. PostgreSQL uses
default privileges for future objects.

Snowflake separates access to the schema from privileges on the objects inside it.
It also separates existing-object grants from future-object grants:

```sql
GRANT USAGE
ON DATABASE analytics
TO ROLE analyst_role;

GRANT USAGE
ON SCHEMA analytics.reporting
TO ROLE analyst_role;

GRANT SELECT
ON ALL TABLES IN SCHEMA analytics.reporting
TO ROLE analyst_role;

GRANT SELECT
ON ALL VIEWS IN SCHEMA analytics.reporting
TO ROLE analyst_role;

GRANT SELECT
ON FUTURE TABLES IN SCHEMA analytics.reporting
TO ROLE analyst_role;

GRANT SELECT
ON FUTURE VIEWS IN SCHEMA analytics.reporting
TO ROLE analyst_role;
```

Assuming the role had no access before these statements, its state afterward is:

| Scope | Object type | Result |
|---|---|---|
| `analytics` | Database | The role can resolve schemas in the database. |
| `analytics.reporting` | Schema | The role can resolve objects in the schema. |
| Existing objects | Tables and views | The role can read the objects that existed when the grants ran. |
| Future objects | Tables and views | The role automatically receives `SELECT` when matching objects are created. |

Materialized views, functions, and sequences can require separate privileges.

### Commit or abandon a transaction

```sql
BEGIN;

UPDATE account_balances
SET
  balance = balance - 25
WHERE
  account_id = 1;

UPDATE account_balances
SET
  balance = balance + 25
WHERE
  account_id = 2;

COMMIT;
```

State before:

| `account_id` | `balance` |
|---:|---:|
| 1 | 100.00 |
| 2 | 50.00 |

State after commit:

| `account_id` | `balance` |
|---:|---:|
| 1 | 75.00 |
| 2 | 75.00 |

`ROLLBACK` would restore the transaction's prior state if the engine and table support
transactions. `SAVEPOINT` marks an intermediate point to which part of a transaction
can be rolled back. Autocommit defaults and DDL transaction behavior vary.

## Data-quality recipes

### Return duplicate keys

```sql
SELECT
  id,
  COUNT(*) AS row_count
FROM
  schema_name.table_name
GROUP BY
  id
HAVING
  COUNT(*) > 1
ORDER BY
  id;
```

Representative result:

| `id` | `row_count` |
|---:|---:|
| 17 | 2 |
| 42 | 3 |

### Enumerate duplicate rows

`ROW_NUMBER`, not `ROW NUMBER`, assigns a deterministic survivor only when its order
contains a stable tie-breaker.

```sql
SELECT
  camis,
  name,
  borough,
  inspection_date,
  violation_code,
  ROW_NUMBER() OVER (
    PARTITION BY
      camis,
      name,
      borough,
      inspection_date,
      violation_code
    ORDER BY
      loaded_at DESC,
      record_id
  ) - 1 AS duplicate_number
FROM
  restaurant_inspections
ORDER BY
  camis,
  duplicate_number;
```

Representative result:

| `camis` | `name` | `borough` | `inspection_date` | `violation_code` | `duplicate_number` |
|---:|---|---|---|---|---:|
| 1001 | Cafe A | Queens | 2026-02-01 | V01 | 0 |
| 1001 | Cafe A | Queens | 2026-02-01 | V01 | 1 |

`duplicate_number = 0` is the chosen survivor; values above zero are duplicate copies
under the stated grain and ordering.

### Choose the value associated with the latest row

Trino's `MAX_BY(x, y)` returns an `x` value associated with the greatest `y`.

```sql
SELECT
  merchant_id,
  MAX_BY(name, observed_at) AS latest_name
FROM
  merchant_names
GROUP BY
  merchant_id
ORDER BY
  merchant_id;
```

Representative result:

| `merchant_id` | `latest_name` |
|---:|---|
| 7 | Northwind Market |

If several rows share the maximum ordering value, the chosen `x` may be
nondeterministic. Make the ordering key unique or use an ordered row-selection
pattern when ties matter.

### Reduce Boolean values

Trino's `BOOL_AND` is true when every non-null input is true. `BOOL_OR` is true when
any non-null input is true. Empty or all-null input normally yields `NULL`.

```sql
SELECT
  BOOL_AND(check_passed) AS every_check_passed,
  BOOL_OR(check_passed) AS any_check_passed
FROM
  validation_results;
```

Representative result:

| `every_check_passed` | `any_check_passed` |
|---|---|
| `FALSE` | `TRUE` |

### Preserve the latest non-null value: invalid and corrected forms

This plausible-looking original is invalid in engines that restrict `FILTER` to
aggregate functions:

```sql
FIRST_VALUE(parent_id)
  FILTER (WHERE parent_id IS NOT NULL)
  OVER (
    PARTITION BY email_domain
    ORDER BY insert_date DESC
  )
```

In Trino, Snowflake, and Redshift, use this placement of `IGNORE NULLS`:

```sql
SELECT
  email_domain,
  FIRST_VALUE(parent_id) IGNORE NULLS OVER (
    PARTITION BY email_domain
    ORDER BY insert_date DESC, record_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS latest_present_parent_id
FROM
  contact_history
ORDER BY
  email_domain,
  insert_date DESC;
```

Representative result:

| `email_domain` | `latest_present_parent_id` |
|---|---:|
| example.com | 81 |
| example.com | 81 |

## Sampling and approximate analysis

### Sample rows or storage blocks in Trino

In Trino, `TABLESAMPLE BERNOULLI(p)` independently considers rows with probability
`p%`. `TABLESAMPLE SYSTEM(p)` samples connector-defined storage units or logical
segments. The requested percentage determines an expected amount, not an exact row
count.

```sql
SELECT
  order_id,
  customer_id,
  amount
FROM
  orders TABLESAMPLE BERNOULLI (40);
```

Representative sample, which can differ on every run:

| `order_id` | `customer_id` | `amount` |
|---:|---:|---:|
| 101 | 1 | 75.00 |
| 104 | 4 | 90.00 |

```sql
SELECT
  o.order_id,
  i.line_number
FROM
  orders AS o TABLESAMPLE SYSTEM (10)
JOIN
  line_items AS i TABLESAMPLE BERNOULLI (40)
  ON i.order_id = o.order_id;
```

Representative sample:

| `order_id` | `line_number` |
|---:|---:|
| 104 | 1 |

For connectors that can avoid reading unsampled segments, system sampling is usually
cheaper but can be biased by physical clustering. Sampling both join inputs
independently can remove matching pairs and distort join metrics.

### Theta sketches

Theta sketches are compact probabilistic summaries of hashed values. They estimate
distinct counts and support approximate union, intersection, and difference without
retaining every original value. They trade exactness for bounded memory and fast set
operations. Whether a query optimizer consumes a user-created sketch is engine- and
feature-specific; do not assume a sketch automatically changes join planning.

## Bit operations and fingerprints

### Test a bit flag

Bit positions are normally zero-based. Prefer integer shift operations over
`POWER(2, position)`, which can introduce floating-point conversion or overflow.
Trino example:

```sql
SELECT
  flags,
  BITWISE_AND(
    flags,
    BITWISE_LEFT_SHIFT(1, 3)
  ) <> 0 AS bit_3_is_set
FROM
  feature_flags;
```

Representative result:

| `flags` | `bit_3_is_set` |
|---:|---|
| 10 | `TRUE` |
| 4 | `FALSE` |

In engines with a `>>` operator, such as PostgreSQL and BigQuery, `id >> 27` means
shift an integer right by 27 bits, not characters. Trino uses
`BITWISE_RIGHT_SHIFT(id, 27)`. For nonnegative integers a right shift resembles
division by `2^27`, but floating-point division plus `FLOOR` is not a universal
replacement, especially for large or negative values.

The original arithmetic approximation was:

```sql
SELECT
  FLOOR(CAST(id AS BIGINT) / POWER(2, 27)) AS shifted_approximately
FROM
  identifiers;
```

Representative result for `id = 268435456`:

| `shifted_approximately` |
|---:|
| 2 |

### BigQuery FarmHash fingerprint

BigQuery's `FARM_FINGERPRINT(value)` returns a signed 64-bit, noncryptographic
FarmHash fingerprint. It is fast and stable for the same input, but collisions remain
possible and it must not be used for secrets or adversarial integrity checks.

```sql
FARM_FINGERPRINT(CAST(customer_id AS STRING))
```

Document the exact input serialization before using a fingerprint as a cross-system
identifier.

## Inspect types and object definitions

### Inspect an expression type

Trino's `TYPEOF` returns the produced type as text.

```sql
SELECT
  TYPEOF(CAST(42 AS BIGINT)) AS column_type;
```

| `column_type` |
|---|
| bigint |

### Show a table definition

The command is engine-specific. Trino uses:

```sql
SHOW CREATE TABLE analytics.external_source.open_bills;
```

Representative result:

| `Create Table` |
|---|
| `CREATE TABLE analytics.external_source.open_bills (...)` |

Amazon Redshift uses `SHOW TABLE`:

```sql
SHOW TABLE analytics.external_source_open_bills;
```

Representative result:

| `ddl` |
|---|
| `CREATE TABLE analytics.external_source_open_bills (...)` |

### Get object DDL in Snowflake

```sql
SELECT
  GET_DDL(
    'TABLE',
    'ANALYTICS.EXTERNAL_SOURCE.OPEN_BILLS'
  ) AS table_ddl;
```

Representative result:

| `table_ddl` |
|---|
| `create or replace TABLE OPEN_BILLS (...)` |

Returned text reflects Snowflake's reconstructed definition, not necessarily the
original formatting.

## Engine-specific maintenance

Trino's Iceberg connector supports table procedures such as file compaction through
`ALTER TABLE ... EXECUTE optimize`. `ANALYZE` collects connector statistics when the
connector supports them.

```sql
ALTER TABLE iceberg.analytics.events
EXECUTE optimize;

ANALYZE iceberg.analytics.events;
```

Compaction rewrites files and consumes compute and input/output (I/O). Run it based on
measured file size, query, and write patterns rather than an unconditional schedule.
Statistics collection behavior, persistence, and cost are connector-specific.

## Related platform behavior

### Snowflake zero-copy cloning

A Snowflake clone initially shares the source object's existing immutable
micro-partitions. Creating the clone is therefore fast and does not duplicate all
stored data upfront.

```text
before clone
source ---> partitions A, B, C

at clone creation
source ---> partitions A, B, C <--- clone

after source changes
source ---> partitions A, B, C2
clone  ---> partitions A, B, C

after clone changes
source ---> partitions A, B, C2
clone  ---> partitions A, B2, C
```

New DML creates new micro-partitions for the changed object; unchanged partitions
remain shared. Storage grows for new or retained partition versions, not as an
immediate full copy. This is useful for development and test environments, but access
control, retention, and the cost of later changes still need explicit design.

### Amazon Web Services (AWS) Glue catalog namespaces

An AWS Glue Data Catalog database is a namespace containing tables. It does not
provide another database or schema level beneath that database. One catalog can have
databases named `prod` and `uat`, each with a table named `payment`, so SQL engines can
expose `prod.payment` and `uat.payment`. If `payment` itself is the database, use
separate catalogs/accounts for environments or flat database names such as
`prod_payment`, `uat_payment`, and `dev_payment`; `prod.payment` cannot mean a database
nested inside another database.

This is catalog organization, not SQL grammar, but it affects the fully qualified
names exposed by engines that use Glue.
