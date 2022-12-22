{{
    config(
        materialized='incremental',
        unique_key = ['Version','country_code','country','Item_key','Item_name','year','month','user']
    )
}}

with load_data_with_country as (
    select * from {{source('country_info','country_codes')}} cross join {{source('item_info','item_prices_stg')}}

)

select Version, Country_key as country_code , Country,Item_key,Item_name,year,month,price,user
from load_data_with_country