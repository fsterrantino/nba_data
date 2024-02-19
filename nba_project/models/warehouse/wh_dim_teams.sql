{{ 
    config(
        materialized='table',
        schema='warehouse'
    ) 
}}

with source_data as (

    select 
        team
    from
        {{ ref('stg_salaries') }}
    order by team
),

unique_teams as (

    SELECT DISTINCT
        team
    FROM
        source_data

),

final as (

    SELECT
        row_number() OVER (ORDER BY team) as team_id,
        team as team_name
    from
        unique_teams

)


select *
from final