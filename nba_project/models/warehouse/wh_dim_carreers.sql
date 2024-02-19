{{ 
    config(
        materialized='table',
        schema='warehouse'
    ) 
}}

with source_data as (

    select 
        _id as player_id,
        nullif(cast(career_AST as string), '-') as career_AST,
        nullif(career_FG_percentage, '-') as career_FG_percentage,
        nullif(career_FG3_percentage, '-') as career_FG3_percentage,
        nullif(career_FT_percentage, '-') as career_FT_percentage,
        nullif(cast(career_G as string), '-') as career_G,
        nullif(cast(career_PER as string), '-') as career_PER,
        nullif(cast(career_PTS as string), '-') as career_PTS,
        nullif(cast(career_TRB as string), '-') as career_TRB,
        nullif(cast(career_WS as string), '-') as career_WS,
        nullif(career_eFG_percentage, '-') as career_eFG_percentage 
    from
        {{ ref('stg_players') }}

),

final as (

    select 
        player_id,
        CAST(career_AST as FLOAT) as career_AST,
        CAST(career_FG_percentage as FLOAT) as career_FG_percentage,
        CAST(career_FG3_percentage as FLOAT) as career_FG3_percentage,
        CAST(career_FT_percentage as FLOAT) as career_FT_percentage,
        CAST(career_G as FLOAT) as career_G,
        CAST(career_PER as FLOAT) as career_PER,
        CAST(career_PTS as FLOAT) as career_PTS,
        CAST(career_TRB as FLOAT) as career_TRB,
        CAST(career_WS as FLOAT) as career_WS,
        CAST(career_eFG_percentage as FLOAT) career_eFG_percentage
    from
        source_data

)

select *
from final