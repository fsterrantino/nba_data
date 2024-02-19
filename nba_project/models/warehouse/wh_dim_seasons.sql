{{ 
    config(
        materialized='table',
        schema='warehouse'
    ) 
}}

with source_data as (

    select distinct 
        season,
        season_start,
        season_end
    from
        {{ ref('stg_salaries') }}

),

final as (

    SELECT
        ROW_NUMBER() OVER (ORDER BY season_start) AS season_id,
        season,
        season_start,
        season_end
    FROM
        source_data

)

select *
from final
