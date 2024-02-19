{{ 
    config(
        materialized='table',
        schema='staging'
    ) 
}}

with source_data as (

    select 
        _id,
        birthDate,
        birthPlace,
        career_AST,
        career_FG_percentage,
        career_FG3_percentage,
        career_FT_percentage,
        career_G,
        career_PER,
        career_PTS,
        career_TRB,
        career_WS,
        career_eFG_percentage,
        college,
        draft_pick,
        draft_round,
        draft_team,
        draft_year,
        height,
        highSchool,
        name,
        position,
        shoots,
        weight
    from
        {{ ref('players') }}
)

select *
from source_data