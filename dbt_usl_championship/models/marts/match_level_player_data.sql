with player_match_stats as (
select *
    , split_part(age, '-', 1)::integer + split_part(age, '-', 2)::integer / 365 as age_numeric
    , min(age_numeric) over (partition by player_id, season_id) as age_at_season_start
    , max(age_numeric) over (partition by player_id, season_id) as age_at_season_end
    , avg(age_numeric) over (partition by player_id, season_id) as average_age_per_season
from {{ ref('base_raw_player_match_stats') }}
)

select 
    player_match_stats.*
    , matches_by_team.match_week_number
    , matces_by_team.season_id
    , matches_by_team.match_date
    , row_number() over (partition by player_match_stats.player_id, matches_by_team.season_id order by matches_by_team.match_date asc) as season_appearances_number
from player_match_stats
left join {{ ref('matches_by_team') }} as matches_by_team
    on player_match_stats.match_id = matches_by_team.match_id
    and player_match_stats.team_name = matches_by_team.team_name