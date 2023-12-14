-- remove outliers from the dataset using inter-quartile range
WITH
    dataset 
AS (
  ...
  ),
    quartiles
AS (
    SELECT
        approx_percentile(dataset_volume, 0.25) AS Q1,
        approx_percentile(dataset_volume, 0.75) AS Q3
    FROM
      dataset
    ),
    iqr_calc
AS (
    SELECT
        Q1,
        Q3,
        Q3 - Q1 AS IQR
    FROM
        quartiles
    ),
    outlier_bounds
AS (
    SELECT
        Q1 - (1.5 * IQR) AS lower_bound,
        Q3 + (1.5 * IQR) AS upper_bound
    FROM
        iqr_calc
    )
SELECT
    o.*
FROM
    dataset o
CROSS JOIN
    outlier_bounds
WHERE
    True
    AND dataset_volume BETWEEN lower_bound AND upper_bound
