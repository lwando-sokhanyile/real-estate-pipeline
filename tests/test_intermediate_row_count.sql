WITH counts AS (

    SELECT
        (SELECT COUNT(*)
         FROM {{ ref('stg_price_paid') }}) AS staging_count,

        (SELECT COUNT(*)
         FROM {{ ref('int_property_transactions') }}) AS intermediate_count

)

SELECT *
FROM counts
WHERE staging_count != intermediate_count