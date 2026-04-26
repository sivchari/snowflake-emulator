{{
    config(
        materialized='table'
    )
}}

SELECT
    id,
    name,
    score,
    score * 2 AS double_score
FROM {{ ref('source_table') }}
WHERE score > 100
