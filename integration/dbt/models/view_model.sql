{{
    config(
        materialized='view'
    )
}}

SELECT
    id,
    name,
    score
FROM {{ ref('source_table') }}
ORDER BY score DESC
