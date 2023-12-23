-- car_type_comparison.sql
{{ config(materialized='table') }}

WITH car_type_aggregates AS (
  SELECT
    car_type,
    AVG(CAST(traveled_distance AS FLOAT)) AS avg_traveled_distance,
    AVG(CAST(avg_speed AS FLOAT)) AS avg_speed
  FROM
    table1  -- this is a reference to the table1 model
  GROUP BY
    car_type
)

SELECT
  car_type,
  avg_traveled_distance,
  avg_speed
FROM
  car_type_aggregates
