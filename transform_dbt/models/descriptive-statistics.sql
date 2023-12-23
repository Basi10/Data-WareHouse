
-- Use the `ref` function to select from other models

{{ config(materialized='table') }}

SELECT
    'table1' AS table_name,
    COUNT(*) AS total_records,
    AVG(CAST(traveled_distance AS FLOAT)) AS mean_traveled_distance,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(traveled_distance AS FLOAT)) AS median_traveled_distance,
    AVG(CAST(avg_speed AS FLOAT)) AS mean_avg_speed,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CAST(avg_speed AS FLOAT)) AS median_avg_speed
FROM
    table1