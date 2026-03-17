with score_parsed as (
    select
        *
        , trim(score, ' ''') as score_clean
    from {{ ref('all_match_stats') }}
),
parsed as (
    select
        *
        -- Penalty format: "(4) 1-1 (2)" → home_penalty=4, home=1, away=1, away_penalty=2
        , case
            when regexp_matches(score_clean, '\(\d+\)\s*\d+-\d+\s*\(\d+\)')
            then regexp_extract(score_clean, '\((\d+)\)\s*(\d+)-(\d+)\s*\((\d+)\)', 1)::int
            else null
          end as home_penalty_shootout_goals
        , case
            when regexp_matches(score_clean, '\(\d+\)\s*\d+-\d+\s*\(\d+\)')
            then regexp_extract(score_clean, '\((\d+)\)\s*(\d+)-(\d+)\s*\((\d+)\)', 2)::int
            else trim(split_part(score_clean, '-', 1))::int
          end as home_team_score
        , case
            when regexp_matches(score_clean, '\(\d+\)\s*\d+-\d+\s*\(\d+\)')
            then regexp_extract(score_clean, '\((\d+)\)\s*(\d+)-(\d+)\s*\((\d+)\)', 3)::int
            else trim(split_part(score_clean, '-', 2))::int
          end as away_team_score
        , case
            when regexp_matches(score_clean, '\(\d+\)\s*\d+-\d+\s*\(\d+\)')
            then regexp_extract(score_clean, '\((\d+)\)\s*(\d+)-(\d+)\s*\((\d+)\)', 4)::int
            else null
          end as away_penalty_shootout_goals
    from score_parsed
)
select
    match_stats.match_id::string as match_id
    , match_stats.season_id::string as season_id
    , coalesce(match_stats.round, 'Regular Season') as game_type
    , match_stats.date::date as match_date
    , cast(match_stats.start_time as time) as match_time
    , case when hour(cast(match_stats.start_time as time)) <= 12 then 'Morning Kickoff'
         when hour(cast(match_stats.start_time as time)) <= 16 then 'Afternoon Kickoff'
         when hour(cast(match_stats.start_time as time)) <= 19 then 'Evening Kickoff'
         when hour(cast(match_stats.start_time as time)) <= 22 then 'Night Kickoff'
         else 'Late Night Kickoff'
         end as match_time_of_day
    , match_stats.home_team as home_team_name
    , match_stats.away_team as away_team_name
    , match_stats.home_team_score
    , match_stats.away_team_score
    , match_stats.home_penalty_shootout_goals
    , match_stats.away_penalty_shootout_goals
    , case when match_stats.home_team_score > match_stats.away_team_score then 'Home Win'
         when match_stats.home_team_score < match_stats.away_team_score then 'Away Win'
         when match_stats.home_team_score = match_stats.away_team_score 
            and match_stats.home_penalty_shootout_goals is null
            and match_stats.away_penalty_shootout_goals is null then 'Draw'
         when match_stats.home_team_score = match_stats.away_team_score 
            and match_stats.home_penalty_shootout_goals > match_stats.away_penalty_shootout_goals then 'Home Win on Penalties'
         when match_stats.home_team_score = match_stats.away_team_score 
            and match_stats.home_penalty_shootout_goals < match_stats.away_penalty_shootout_goals then 'Away Win on Penalties'
        when match_stats.home_team_score is null 
            and match_stats.away_team_score is null
            and (match_stats.notes ilike '%match cancelled%' 
                or match_stats.notes ilike '%match canceled%') then 'Cancelled'
         else 'Unknown'
         end as match_result
    , match_stats.attendance
    , match_stats.referee as match_referee_name
    , match_stats.venue as match_venue
    , match_stats.match_report as match_report_url
    , match_stats.notes as match_notes
from parsed as match_stats
