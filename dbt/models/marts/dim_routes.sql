/*
  dim_routes.sql

  One row per route. Route metadata sourced from stg_bus_line (ArcGIS snapshots).

  Join strategy: take the most recent snapshot available per route (2024 first,
  falling back to 2023 → 2022 → 2019). This preserves metadata for discontinued
  routes (they appear only in earlier snapshots) while giving active routes the
  latest known attributes.
*/

with

-- Rank each route's snapshots: latest year = rank 1
ranked as (
    select
        route_number,
        line_raw,
        line_name,
        vehicle_type,
        service_type,
        subregion,
        subregion_raw,
        corridor_population,
        corridor_employment,
        avg_trip_dist_km,
        shape_length,
        annual_service_cost,
        annual_service_hours,
        snapshot_year,
        note,
        row_number() over (
            partition by route_number
            order by snapshot_year desc
        ) as _row_num
    from {{ ref('stg_bus_line') }}
    where route_number is not null
),

-- Keep only the most recent snapshot per route
latest as (
    select * from ranked where _row_num = 1
),

-- Derive a cleaned display name: use line_name when available, else route_number
final as (
    select
        route_number,
        line_raw                                        as route_number_raw,
        coalesce(nullif(trim(line_name), ''), route_number) as route_name,
        coalesce(nullif(trim(vehicle_type), ''), 'Unknown') as vehicle_type,
        coalesce(nullif(trim(service_type), ''), 'Unknown') as service_type,
        subregion,
        subregion_raw,
        corridor_population,
        corridor_employment,
        avg_trip_dist_km,
        shape_length,
        annual_service_cost,
        annual_service_hours                            as annual_service_hours_latest,
        snapshot_year                                   as latest_snapshot_year,
        note
    from latest
)

select * from final
