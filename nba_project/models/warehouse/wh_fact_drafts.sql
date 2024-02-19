{{ 
    config(
        materialized='table',
        schema='warehouse'
    ) 
}}

with source_data as (

    select
        '' as draft_id, -- SK
        '' as league_id,
        stg_salaries.league as league_name,
        stg_players._id as player_id,
        wh_dim_teams.team_id,
        '' as season_id,
        stg_salaries.season as season,
        stg_salaries.salary,
        stg_players.draft_pick,
        stg_players.draft_round,
        stg_players.draft_team,
        stg_players.draft_year
    from
        {{ ref('stg_players') }} stg_players
        join
            {{ ref('wh_dim_teams') }} wh_dim_teams
            ON stg_players.draft_team = wh_dim_teams.team_name
        join
            {{ ref('stg_salaries') }} stg_salaries
            ON stg_players._id = stg_salaries.player_id
            AND stg_players.draft_year = stg_salaries.season_start

),

final as (

    select
        ROW_NUMBER() OVER (ORDER BY source_data.draft_year) as draft_id, -- SK
        wh_dim_leagues.league_id as league_id,
        source_data.player_id,
        source_data.team_id,
        wh_dim_seasons.season_id as season_id,
        source_data.salary,
        source_data.draft_pick,
        source_data.draft_round,
        source_data.draft_team,
        source_data.draft_year
    from
        source_data
    JOIN
        {{ ref('wh_dim_leagues') }} wh_dim_leagues
        ON wh_dim_leagues.league_name = source_data.league_name
    JOIN
        {{ ref('wh_dim_seasons') }} wh_dim_seasons
        ON wh_dim_seasons.season = source_data.season

)

select *
from final
