SELECT
    transfer_year,
    transfer_month,
    property_type_name
FROM {{ ref('mart_property_type_trends') }}
GROUP BY
    transfer_year,
    transfer_month,
    property_type_name
HAVING COUNT(*) > 1