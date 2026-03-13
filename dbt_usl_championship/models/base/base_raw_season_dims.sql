WITH source AS (
    SELECT *
    FROM {{ source('raw', 'source_season_dims') }}
)

SELECT * FROM source