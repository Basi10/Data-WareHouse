-- hotspots.sql
{{ config(materialized='table') }}

WITH trajectory_data AS (
  SELECT
    t1.track_id,
    t1.car_type,
    t1.traveled_distance,
    t1.avg_speed,
    t2.lat,
    t2.lon,
    t2.speed,
    t2.lon_acc,
    t2.lat_acc,
    t2.time
  FROM
    table1 t1
  JOIN
    table2 t2 ON t1.track_id = t2.track_id
)

SELECT
  lat,
  lon,
  AVG(speed) AS avg_speed,
  COUNT(*) AS point_count
FROM
  trajectory_data
GROUP BY
  lat, lon
HAVING
  COUNT(*) > 10  -- Adjust as needed to define what constitutes a hotspot
ORDER BY
  avg_speed DESC
LIMIT 10
