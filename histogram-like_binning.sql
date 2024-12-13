-- histogram-like binning of payment amounts on a logarithmic scale, ensuring exponential ranges such as 1-2, 2-4, 4-8, and so on.
SELECT
    pow(2, floor(ln(amount) / ln(2))) AS pmtBin,
    pmtOutcome,
    count(amount)                     AS nPmts
FROM
    payments
GROUP BY
    1, 2
