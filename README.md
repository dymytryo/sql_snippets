# SQL snippets

Reusable SQL reference material and query patterns. Examples span portable SQL and
several explicitly named engines, including Trino/Starburst, Snowflake, PostgreSQL,
Amazon Redshift, MySQL, and BigQuery.

## Reference

- [`sql_reference.md`](sql_reference.md): SQL execution, querying, joins, set
  operations, aggregation, windows, strings, dates, semi-structured data, DML, DDL,
  permissions, sampling, metadata, and engine-specific behavior.

## Query examples

| File | Covers |
|---|---|
| [`asof_join.sql`](asof_join.sql) | Nearest-row matching with an `ASOF JOIN` and a window-function alternative |
| [`find_duplicated_columns_in_postgres.sql`](find_duplicated_columns_in_postgres.sql) | Duplicate column-name inspection through PostgreSQL metadata |
| [`histogram-like_binning.sql`](histogram-like_binning.sql) | Logarithmic numeric bins |
| [`pick_best_merchant_name.sql`](pick_best_merchant_name.sql) | Merchant-name normalization and preferred-value selection |
| [`previous_week_window.sql`](previous_week_window.sql) | Prior-period measures with window frames |
| [`qualify.sql`](qualify.sql) | Latest-row selection with `QUALIFY` |
| [`remove_outliers.sql`](remove_outliers.sql) | Interquartile-range outlier filtering |
| [`reworking_query.sql`](reworking_query.sql) | Original and revised dbt incremental-model approaches |
| [`table_sample.sql`](table_sample.sql) | `BERNOULLI` table sampling before joins |
| [`unit_flow.md`](unit_flow.md) and [`unit_flow.sql`](unit_flow.sql) | Beginning, new, revived, transferred, attrited, and ending customer units |
