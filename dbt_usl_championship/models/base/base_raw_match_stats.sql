WITH source AS (
    SELECT *
    FROM {{ source('raw', 'source_match_stats') }}
)

SELECT * FROM source