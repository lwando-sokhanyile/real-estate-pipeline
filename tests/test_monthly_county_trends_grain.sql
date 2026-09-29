SELECT
    transfer_year,
    transfer_month,
    county
FROM {{ ref('mart_monthly_county_trends') }}
GROUP BY
    transfer_year,
    transfer_month,
    county
HAVING COUNT(*) > 1