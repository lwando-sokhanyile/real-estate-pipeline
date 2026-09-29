SELECT
    county
FROM {{ ref('mart_county_market_summary') }}
GROUP BY county
HAVING COUNT(*) > 1
