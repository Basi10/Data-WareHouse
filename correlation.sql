-- correlation_analysis.sql
-- correlation_analysis.sql
{{ config(materialized='table') }}

WITH joined_data AS (
  SELECT
    t1.track_id,
    t1.traveled_distance,
    t1.avg_speed,
    t2.speed,
    t2.lon_acc,
    t2.lat_acc
  FROM
    table1 t1
  JOIN
    table2 t2 ON t1.track_id = t2.track_id
)

SELECT
  CORR(CAST(traveled_distance AS FLOAT), CAST(avg_speed AS FLOAT)) AS correlation_distance_speed,
  CORR(speed, lon_acc) AS correlation_speed_lon_acc,
  CORR(speed, lat_acc) AS correlation_speed_lat_acc
FROM
  joined_data

