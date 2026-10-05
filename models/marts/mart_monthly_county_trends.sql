select
    transfer_year,
    transfer_month,
    county,
    transaction_count,
    average_sale_price,
    median_sale_price

from {{ ref('int_monthly_county_metrics') }}

order by
    transfer_year,
    transfer_month,
    county
