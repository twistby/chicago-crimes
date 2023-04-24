{{ config(materialized="table") }}

SELECT 
  c.id AS crime_id,
  s.school_id AS school_id,
  ST_DISTANCE(ST_GeogPoint(c.longitude, c.latitude), ST_GeogPoint(s.long, s.lat)) AS distance
FROM 
  {{ ref("stg_street_crimes") }}  c
JOIN 
  {{ source("staging", "schools") }}  s 
ON 
  ST_DISTANCE(ST_GeogPoint(c.longitude, c.latitude), ST_GeogPoint(s.long, s.lat)) < 500
ORDER BY 
  c.id, 
  distance 