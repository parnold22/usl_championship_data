
select player_match_stats.*
    , matches_by_team.match_week_number
    , matches_by_team.season_id
    , matches_by_team.match_date
    , split_part(player_match_stats.age, '-', 1)::integer + split_part(player_match_stats.age, '-', 2)::integer / 365 as age_numeric
    , min(age_numeric) over (partition by player_match_stats.player_id, matches_by_team.season_id) as age_at_season_start
    , max(age_numeric) over (partition by player_match_stats.player_id, matches_by_team.season_id) as age_at_season_end
    , avg(age_numeric) over (partition by player_match_stats.player_id, matches_by_team.season_id) as average_age_per_season
    , row_number() over (partition by player_match_stats.player_id, matches_by_team.season_id order by matches_by_team.match_date asc) as season_appearances_number
from {{ ref('base_raw_player_match_stats') }} as player_match_stats
left join {{ ref('matches_by_team') }} as matches_by_team
    on player_match_stats.match_id = matches_by_team.match_id
    and player_match_stats.team_name = matches_by_team.team_name
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35
