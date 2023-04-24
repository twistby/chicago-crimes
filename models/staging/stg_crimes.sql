{{config(materialized='view')}}

SELECT 
    id, 
    date, 
    primary_type, 
    description, 
    CONCAT(primary_type, ' ', description) AS full_description, 
    location_description, 
    year, 
    latitude, 
    longitude 
FROM {{ source('staging','crimes') }}
WHERE latitude != 0 and longitude!= 0 and location_description = 'STREET' 
LIMIT 1000
