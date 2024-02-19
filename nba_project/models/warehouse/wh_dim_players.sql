{{ 
    config(
        materialized='table',
        schema='warehouse'
    ) 
}}

with source_data as (

    select 
        _id as player_id,
        name as player_name,
        birthPlace as player_birth_place,
        TO_CHAR(TO_DATE(birthDate, 'Mon DD, YYYY'), 'DD/MM/YYYY') as player_birth_date,
        CAST(SPLIT_PART(height, '-', 1) AS INT) AS height_feet,
        CAST(SPLIT_PART(height, '-', 2) AS INT) AS height_inches,
        CAST(LEFT(weight, LENGTH('240lb') - 2) AS INTEGER) as player_weight_lb,
        college as player_college,
        shoots as player_shooting_hand,
        position as player_position
    from
        {{ ref('stg_players') }}
)

select *
from source_data