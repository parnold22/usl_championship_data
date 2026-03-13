WITH source AS (
    SELECT *
    FROM {{ source('raw', 'source_player_match_stats') }}
)

SELECT * FROM source