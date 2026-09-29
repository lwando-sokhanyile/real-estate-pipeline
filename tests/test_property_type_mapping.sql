SELECT *
FROM {{ ref('int_property_transactions') }}
WHERE
       (property_type = 'D' AND property_type_name != 'detached')
    OR (property_type = 'F' AND property_type_name != 'flat_maisonette')
    OR (property_type = 'O' AND property_type_name != 'other')
    OR (property_type = 'S' AND property_type_name != 'semi_detached')
    OR (property_type = 'T' AND property_type_name != 'terraced')