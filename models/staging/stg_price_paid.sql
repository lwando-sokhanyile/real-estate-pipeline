with source as (

    select
        transaction_id,
        price,
        transfer_date,
        postcode,
        property_type,
        old_new,
        duration,
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
    from staging.price_paid

),

renamed as (

    select
        transaction_id,
        price,
        transfer_date,

        postcode,
        property_type,
        old_new,
        duration,

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

    from source

)

select *
from renamed