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
    , row_number() over (partition by matches.team_id, matches.season_id order by matches.match_date asc) as match_week_number
from matches
)

select * 
from match_order