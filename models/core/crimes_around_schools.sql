{{ config(materialized="table") }}

select
    n.crime_id,
    n.school_id,
    n.distance,
    c.date,
    c.primary_type,
    c.description,
    c.full_description,
    c.year,
    s.short_name,
    s.address,
    s.grade_cat
from {{ ref("stg_nearest_schools") }} n
inner join {{ ref("stg_crimes") }} c on n.crime_id = c.id
inner join {{ source("staging", "schools") }} s on n.school_id = s.school_id
