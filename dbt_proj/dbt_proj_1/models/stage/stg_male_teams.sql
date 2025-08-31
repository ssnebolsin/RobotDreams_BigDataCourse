{{ config(
    materialized='table',
    tags=['stage', 'male', 'teams']
) }}


with source as (
    select * from {{ source('public', 'male_teams') }}
)
select
    team_id,
    team_url,
    fifa_version,
    fifa_update,
    fifa_update_date,
    team_name,
    league_id,
    league_name,
    nationality_id,
    nationality_name,
    overall,
    attack,
    midfield,
    defence,
    coach_id,
    home_stadium,
    rival_team,
    international_prestige,
    domestic_prestige,
    club_worth_eur,
    starting_xi_average_age,
    whole_team_average_age,
    captain,
    short_free_kick,
    long_free_kick,
    left_short_free_kick,
    right_short_free_kick,
    penalties,
    left_corner,
    right_corner,
    def_style,
    def_team_width,
    def_team_depth,
    off_build_up_play,
    off_chance_creation,
    off_team_width,
    off_players_in_box,
    off_corners,
    off_free_kicks
from source
where league_level is not null

