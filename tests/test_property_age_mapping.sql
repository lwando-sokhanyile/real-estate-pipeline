SELECT *
FROM {{ ref('int_property_transactions') }}
WHERE
       (old_new = 'Y' AND property_age_category != 'new_build')
    OR (old_new = 'N' AND property_age_category != 'established')