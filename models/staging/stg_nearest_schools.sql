{{ config(materialized="view") }}

select
    c.id as crime_id,
    s.school_id as school_id,
    round(
        st_distance(st_geogpoint(c.longitude, c.latitude), st_geogpoint(s.long, s.lat))
    ) as distance
from {{ ref("stg_crimes") }} c
join {{ source("staging", "schools") }} s on 1 = 1
where
    st_distance(st_geogpoint(c.longitude, c.latitude), st_geogpoint(s.long, s.lat))
    < 1000
order by crime_id, distance asc
limit 1000
