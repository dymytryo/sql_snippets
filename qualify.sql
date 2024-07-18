-- Get the most recent invite for a given vendor
SELECT
    merchantId,
    createdDate,
    type,
    status,
    id AS cerrentInviteId,
    FIRST_VALUE(id) OVER (PARTITION BY merchantId
                              ORDER BY createdDate ASC
                          ROWS BETWEEN UNBOUNDED PRECEDING
                                   AND UNBOUNDED FOLLOWING
    )  AS initialInviteId,
    COUNT(*) OVER (PARTITION BY merchantId
    ) AS inviteCount
FROM
    aws_glue.merchantInvite
WHERE
    True
QUALIFY
    row_number() OVER (
                        PARTITION BY merchantId
                            ORDER BY createdDate DESC
                        ) = 1
