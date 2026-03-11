/*
  dim_subregions.sql

  One row per Metro Vancouver sub-region. Sourced from the subregion_population
  seed CSV (2021 census). Adds population density for geographic context.
*/

with

seed_data as (
    select * from {{ ref('subregion_population') }}
),

final as (
    select
        subregion_name,
        subregion_code,
        population_2021,
        area_km2,
        -- Persons per km² — useful for normalising demand metrics
        round(population_2021 / nullif(area_km2, 0), 1) as population_density_per_km2
    from seed_data
)

select * from final
