WITH reconciliation AS (
    SELECT
        (
            SELECT SUM(transaction_count)
            FROM {{ ref('mart_monthly_county_trends') }}
        ) AS mart_transaction_count,

        (
            SELECT COUNT(*)
            FROM {{ ref('int_property_transactions') }}
        ) AS intermediate_transaction_count
)
SELECT *
FROM reconciliation
WHERE mart_transaction_count != intermediate_transaction_count