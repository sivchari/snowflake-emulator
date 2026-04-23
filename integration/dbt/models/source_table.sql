{{
    config(
        materialized='table'
    )
}}

SELECT 1 AS id, 'Alice' AS name, 100 AS score
UNION ALL
SELECT 2 AS id, 'Bob' AS name, 200 AS score
UNION ALL
SELECT 3 AS id, 'Charlie' AS name, 300 AS score
