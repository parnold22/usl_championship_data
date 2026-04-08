with teams as (
select distinct home_team_name as team_name
from {{ ref('dim_match') }}
union all
select distinct away_team_name as team_name
from {{ ref('dim_match') }}
)
, distinct_teams as (
select distinct team_name
    , {{ dbt_utils.generate_surrogate_key(['team_name']) }} as team_id
from teams
)
, matches as (
select distinct_teams.team_id
    , distinct_teams.team_name
    , dim_match.match_id
    , dim_match.match_date
    , dim_match.match_time
    , dim_match.season_id
    , split_part(dim_match.season_id, '_', 2) as season
    , dim_match.game_type
    , case when distinct_teams.team_name = dim_match.home_team_name then 'Home' 
        else 'Away' 
        end as match_location
    , case when distinct_teams.team_name = dim_match.home_team_name then dim_match.away_team_name 
        else dim_match.home_team_name 
        end as opponent_team_name
    , case when distinct_teams.team_name = dim_match.home_team_name 
            and (dim_match.match_result = 'Home Win' 
                or dim_match.match_result = 'Home Win on Penalties') then 'Win'
        when distinct_teams.team_name = dim_match.away_team_name 
            and (dim_match.match_result = 'Away Win' 
                or dim_match.match_result = 'Away Win on Penalties') then 'Win'
        when distinct_teams.team_name = dim_match.home_team_name 
            and dim_match.match_result = 'Draw' then 'Draw'
        when distinct_teams.team_name = dim_match.away_team_name 
            and dim_match.match_result = 'Draw' then 'Draw'
        when distinct_teams.team_name = dim_match.home_team_name 
            and (dim_match.match_result = 'Away Win' 
                or dim_match.match_result = 'Away Win on Penalties') then 'Loss'
        when distinct_teams.team_name = dim_match.away_team_name 
            and (dim_match.match_result = 'Home Win' 
                or dim_match.match_result = 'Home Win on Penalties') then 'Loss'
        else 'Unknown'
        end as match_outcome
    , case when distinct_teams.team_name = dim_match.home_team_name then dim_match.home_team_score
        when distinct_teams.team_name = dim_match.away_team_name then dim_match.away_team_score
        else 0
        end as goals_scored
    , case when distinct_teams.team_name = dim_match.home_team_name then dim_match.away_team_score
        when distinct_teams.team_name = dim_match.away_team_name then dim_match.home_team_score
        else 0
        end as goals_conceded
    , case when dim_match.game_type = 'Regular Season' and match_outcome = 'Win' then 3
        when dim_match.game_type = 'Regular Season' and match_outcome = 'Draw' then 1
        else 0
        end as match_points
from distinct_teams
left join {{ ref('dim_match') }} as dim_match 
    on distinct_teams.team_name = dim_match.home_team_name 
    or distinct_teams.team_name = dim_match.away_team_name
where dim_match.game_type is not null
)
, match_order as (
select matches.*
    , row_number() 
        over (partition by matches.team_id, matches.season_id order by matches.match_date asc) as match_week_number
    , sum(matches.match_points) 
        over (partition by matches.team_id, matches.season_id order by matches.match_date asc) as season_points
    , sum(case when matches.match_location = 'Home' then matches.match_points else 0 end) 
        over (partition by matches.team_id, matches.season_id order by matches.match_date asc) as season_points_home
    , sum(case when matches.match_location = 'Away' then matches.match_points else 0 end) 
        over (partition by matches.team_id, matches.season_id order by matches.match_date asc) as season_points_away
    , sum(matches.goals_scored) 
        over (partition by matches.team_id, matches.season_id order by matches.match_date asc) as season_goals_scored
    , sum(matches.goals_conceded) 
        over (partition by matches.team_id, matches.season_id order by matches.match_date asc) as season_goals_conceded
    , season_goals_scored - season_goals_conceded as season_goal_difference
from matches
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
)
/* cum_non_wins: segments for consecutive wins. cum_wins: segments for consecutive winless (non-wins). */
, match_order_with_cum as (
select match_order.*
    , sum(case when match_order.match_outcome != 'Win' then 1 else 0 end) 
        over (partition by match_order.team_id, match_order.season_id order by match_order.match_date asc) as cum_non_wins
    , sum(case when match_order.match_outcome = 'Win' then 1 else 0 end) 
        over (partition by match_order.team_id, match_order.season_id order by match_order.match_date asc) as cum_wins
from match_order
)
, season_positions as (
select match_order_with_cum.* exclude (cum_non_wins, cum_wins)
    , row_number() 
        over (partition by season_id, match_week_number order by season_points desc, season_goal_difference desc) as current_match_week_position
    , row_number() 
        over (partition by season_id, match_week_number order by season_points_home desc) as current_match_week_position_home
    , row_number() 
        over (partition by season_id, match_week_number order by season_points_away desc) as current_match_week_position_away
    , row_number() 
        over (partition by season_id, match_week_number order by season_goals_scored desc) as current_match_week_position_goals_scored_best_offense
    , row_number() 
        over (partition by season_id, match_week_number order by season_goals_scored asc) as current_match_week_position_goals_scored_worst_offense
    , row_number() 
        over (partition by season_id, match_week_number order by season_goals_conceded desc) as current_match_week_position_goals_conceded_worst_defence -- spelling mistake but keeping since it because there is no replace references function in Tableau public which makes correcting calcs using this field time consuming
    , row_number() 
        over (partition by season_id, match_week_number order by season_goals_conceded asc) as current_match_week_position_goals_conceded_best_defence -- spelling mistake and keeping for same reason
    , case when match_order_with_cum.match_outcome = 'Win' then
        sum(case when match_order_with_cum.match_outcome = 'Win' then 1 else 0 end) 
            over (partition by match_order_with_cum.team_id, match_order_with_cum.season_id, match_order_with_cum.cum_non_wins order by match_order_with_cum.match_date asc)
        else 0
        end as consecutive_win_count
    , case when match_order_with_cum.match_outcome != 'Win' then
        sum(case when match_order_with_cum.match_outcome != 'Win' then 1 else 0 end) 
            over (partition by match_order_with_cum.team_id, match_order_with_cum.season_id, match_order_with_cum.cum_wins order by match_order_with_cum.match_date asc)
        else 0
        end as consecutive_winless_count
from match_order_with_cum
)

select * 
from season_positions
