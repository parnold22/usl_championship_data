with primary_position_by_seaon as (
select player_match_stats.player_id
    , matches_by_team.season_id
    , player_match_stats.primary_position
    , sum(player_match_stats.minutes) as total_minutes
    , row_number() over (partition by player_match_stats.player_id, matches_by_team.season_id order by sum(player_match_stats.minutes) desc) as season_primary_position_rank
from {{ ref('all_player_match_stats') }} as player_match_stats
left join {{ ref('matches_by_team') }} as matches_by_team
    on player_match_stats.match_id = matches_by_team.match_id
    and player_match_stats.team_name = matches_by_team.team_name
group by 1, 2, 3
)
, match_level_player_data as (
select player_match_stats.*
    , primary_position_by_seaon.primary_position as season_primary_position
    , matches_by_team.match_week_number
    , matches_by_team.season_id
    , matches_by_team.match_date
    , try_cast(split_part(player_match_stats.age, '-', 1) as integer) + try_cast(split_part(player_match_stats.age, '-', 2) as integer) / 365 as age_numeric
    , count( distinct season_primary_position_count.primary_position) as season_primary_position_count
    , min(age_numeric) over (partition by player_match_stats.player_id, matches_by_team.season_id) as age_at_season_start
    , max(age_numeric) over (partition by player_match_stats.player_id, matches_by_team.season_id) as age_at_season_end
    , avg(age_numeric) over (partition by player_match_stats.player_id, matches_by_team.season_id) as average_age_per_season
    , row_number() over (partition by player_match_stats.player_id, matches_by_team.season_id order by matches_by_team.match_date asc) as season_appearances_number
    , sum(player_match_stats.goals) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_goals_scored
    , sum(player_match_stats.assists) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_assists
    , sum(player_match_stats.interceptions) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_interceptions
    , sum(player_match_stats.fouls) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_fouls_committed
    , sum(player_match_stats.fouled) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_fouls_drawn
    , sum(player_match_stats.shots) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_shots
    , sum(player_match_stats.shots_on_target) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_shots_on_target
    , sum(player_match_stats.cards_yellow) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_yellow_cards
    , sum(player_match_stats.cards_red) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_red_cards
    , sum(player_match_stats.minutes) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_minutes_played
    , sum(player_match_stats.tackles_won) over (partition by player_match_stats.player_id, matches_by_team.season_id, player_match_stats.team_name) as season_tackles_won
from {{ ref('all_player_match_stats') }} as player_match_stats
left join {{ ref('matches_by_team') }} as matches_by_team
    on player_match_stats.match_id = matches_by_team.match_id
    and player_match_stats.team_name = matches_by_team.team_name
left join primary_position_by_seaon as primary_position_by_seaon
    on player_match_stats.player_id = primary_position_by_seaon.player_id
    and matches_by_team.season_id = primary_position_by_seaon.season_id
    and primary_position_by_seaon.season_primary_position_rank = 1
left join primary_position_by_seaon as season_primary_position_count
    on player_match_stats.player_id = season_primary_position_count.player_id
    and matches_by_team.season_id = season_primary_position_count.season_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36
)
, appending_team_level_stat_ranks as (
select match_level_player_data.*
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_goals_scored desc, match_level_player_data.player asc) as season_goals_scored_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_assists desc, match_level_player_data.player asc) as season_assists_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_interceptions desc, match_level_player_data.player asc) as season_interceptions_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_fouls_committed desc, match_level_player_data.player asc) as season_fouls_committed_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_fouls_drawn desc, match_level_player_data.player asc) as season_fouls_drawn_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_shots desc, match_level_player_data.player asc) as season_shots_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_shots_on_target desc, match_level_player_data.player asc) as season_shots_on_target_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_yellow_cards desc, match_level_player_data.player asc) as season_yellow_cards_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_red_cards desc, match_level_player_data.player asc) as season_red_cards_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_minutes_played desc, match_level_player_data.player asc) as season_minutes_played_rank
    , dense_rank() over (partition by match_level_player_data.team_name, match_level_player_data.season_id order by match_level_player_data.season_tackles_won desc, match_level_player_data.player asc) as season_tackles_won_rank
from match_level_player_data
)

select * from appending_team_level_stat_ranks