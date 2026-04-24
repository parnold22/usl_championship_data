/* the data structure here will look a bit odd as we need one row per team per season per metric to be able to compute create the appropriatte 
visualization in tableau. */
with team_end_of_regular_season_data as (
select team_name
    , team_id
    , season_id
    , max(match_week_number) as match_count
    , max_by(current_match_week_position, match_week_number) as regular_season_end_position
    , max_by(season_points, match_week_number) as regular_season_end_points
    , regular_season_end_points / match_count as regular_season_end_points_per_match
    , max_by(season_goal_difference, match_week_number) as regular_season_end_goal_difference
    , max_by(season_goals_scored, match_week_number) as regular_season_end_goals_scored
    , max_by(current_match_week_position_goals_scored_best_offense, match_week_number) as regular_season_end_goals_scored_rank
    , regular_season_end_goals_scored / match_count as regular_season_end_goals_scored_per_match
    , max_by(season_goals_conceded, match_week_number) as regular_season_end_goals_conceded
    , max_by(current_match_week_position_goals_conceded_best_defence, match_week_number) as regular_season_end_goals_conceded_rank
    , regular_season_end_goals_conceded / match_count as regular_season_end_goals_conceded_per_match
    , max(consecutive_win_count) as regular_season_longest_consecutive_win_count
    , count( distinct case when match_outcome = 'Win' then match_id end) / count(distinct match_id) as regular_season_win_percentage
from {{ ref('matches_by_team') }} as matches_by_team
where game_type = 'Regular Season'
and split_part(season_id, '_', 2)::int < year(current_date)
group by all
)
, playoff_history as (
select team_name
    , team_id
    , season_id
    , min_by(match_location, match_date) as first_playoff_round_match_location
    , count( distinct case when match_location = 'Home' then match_id end) / count(distinct match_id) as playoff_run_home_percentage
    , max_by(game_type, match_date) as playoff_run_latest_round
    , max_by(match_outcome, match_date) as playoff_run_latest_round_match_outcome
from {{ ref('matches_by_team') }} as matches_by_team
where game_type <> 'Regular Season'
and split_part(season_id, '_', 2)::int < year(current_date)
group by all
)
, append_team_playoff_data as (
select team_end_of_regular_season_data.*
    , if(playoff_history.first_playoff_round_match_location = 'Home', 1, 0) as first_playoff_round_match_location_is_home
    , playoff_history.playoff_run_home_percentage
    , case when playoff_history.playoff_run_latest_round = 'Final' and playoff_history.playoff_run_latest_round_match_outcome = 'Win' then 'Playoff Champions'
        when playoff_history.playoff_run_latest_round is not null then concat('Lost in ', playoff_history.playoff_run_latest_round)
        else 'Missed Playoffs'
        end as playoff_result
    , case when playoff_history.playoff_run_latest_round = 'Final' and playoff_history.playoff_run_latest_round_match_outcome = 'Win' then 1 else 0 end as is_playoff_champion
from team_end_of_regular_season_data as team_end_of_regular_season_data
left join playoff_history as playoff_history
    on team_end_of_regular_season_data.team_id = playoff_history.team_id
    and team_end_of_regular_season_data.season_id = playoff_history.season_id
)

select * from append_team_playoff_data where playoff_result <> 'Missed Playoffs'