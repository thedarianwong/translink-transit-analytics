/*
  stg_stops.sql

  Cleans and casts the GTFS stops table.

  Filters out parent stations (location_type = 1) to keep only boardable stops
  (location_type = 0 or NULL). TransLink uses parent stations for SkyTrain
  stations that group platform stops — we only need the leaf stops for
  geospatial context in the bus analysis.
*/

with

source as (
    select * from {{ source('raw', 'RAW_GTFS_STOPS') }}
),

casted as (
    select
        trim("stop_id")                             as stop_id,
        try_to_number("stop_code")                  as stop_code,
        trim("stop_name")                           as stop_name,
        trim("stop_desc")                           as stop_desc,
        try_to_double("stop_lat")                   as stop_lat,
        try_to_double("stop_lon")                   as stop_lon,
        trim("zone_id")                             as zone_id,
        trim("stop_url")                            as stop_url,
        coalesce(try_to_number("location_type"), 0) as location_type,
        trim("parent_station")                      as parent_station,
        try_to_number("wheelchair_boarding")        as wheelchair_boarding,
        trim("stop_timezone")                       as stop_timezone
    from source
),

filtered as (
    select *
    from casted
    -- Keep boardable stops only; exclude parent station groupings
    where location_type = 0
      -- Exclude stops with missing or clearly invalid coordinates
      and stop_lat is not null
      and stop_lon is not null
      and stop_lat between 48.0 and 50.0    -- Metro Vancouver bounding box
      and stop_lon between -124.0 and -121.5
)

select * from filtered
