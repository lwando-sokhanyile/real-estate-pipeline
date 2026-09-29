WITH monthly_rank_checks AS (
    SELECT
        transfer_year,
        transfer_month,
        MIN(transaction_rank) AS min_transaction_rank,
        MIN(price_rank) AS min_price_rank
    FROM {{ ref('mart_market_rankings') }}
    GROUP BY
        transfer_year,
        transfer_month
)
SELECT *
FROM monthly_rank_checks
WHERE min_transaction_rank != 1
   OR min_price_rank != 1