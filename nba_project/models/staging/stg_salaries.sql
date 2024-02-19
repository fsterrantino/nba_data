{{ 
    config(
        materialized='table',
        schema='staging'
    ) 
}}

with source_data as (

    select 
        league,
        player_id,
        salary,
        season,
        season_end,
        season_start,
        team
    from
        {{ ref('salaries') }}
)

select *
from source_data