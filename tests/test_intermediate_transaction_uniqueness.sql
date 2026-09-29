SELECT
    transaction_id
FROM {{ ref('int_property_transactions') }}
GROUP BY transaction_id
HAVING COUNT(*) > 1