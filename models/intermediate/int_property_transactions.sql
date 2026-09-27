
with transactions as (

    select *
    from {{ ref('stg_price_paid') }}

),

enriched as (

    select
        transaction_id,
        price,
        transfer_date,
        postcode,

        property_type,

        case property_type
            when 'D' then 'detached'
            when 'S' then 'semi_detached'
            when 'T' then 'terraced'
            when 'F' then 'flat_maisonette'
            when 'O' then 'other'
            else 'unknown'
        end as property_type_name,

        old_new,

        case old_new
            when 'Y' then 'new_build'
            when 'N' then 'established'
            else 'unknown'
        end as property_age_category,

        duration,

        case duration
            when 'F' then 'freehold'
            when 'L' then 'leasehold'
            else 'unknown'
        end as tenure_type,

        paon,
        saon,
        street,
        locality,
        town_city,
        district,
        county,
        ppd_category,
        record_status,
        transfer_year,
        transfer_month

    from transactions

)

select *
from enriched