{{
    config(
        materialized='table'
    )
}}

SELECT
    COUNT(*) AS total_count,
    SUM(score) AS total_score
FROM {{ ref('source_table') }}
