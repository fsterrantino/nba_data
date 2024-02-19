{{ 
    config(
        materialized='table',
        schema='warehouse'
    ) 
}}

with source_data as (

    select distinct 
        league
    from
        {{ ref('stg_salaries') }}

),

final as (

    SELECT
        ROW_NUMBER() OVER (ORDER BY league) AS league_id,
        league as league_name
    FROM
        source_data

)

select *
from final
