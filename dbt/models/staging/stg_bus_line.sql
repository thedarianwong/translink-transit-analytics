/*
  stg_bus_line.sql

  Unions the four ArcGIS "Bus Line with Connections" snapshots (2019, 2022,
  2023, 2024). These files contain richer route metadata that the yearline
  files lack: route name, sub-region, service type, cost, route length.

  Numeric fields in the ArcGIS export have unit suffixes like "85 km/h" or
  "12%". These are stripped with REGEXP_REPLACE before casting.

  Sub-region names are standardised to a fixed 7-category set matching the
  subregion_population seed CSV.

  One row per route per TSPR snapshot year.
*/

with

arcgis_2019 as (
    select *, '2019' as snapshot_year from {{ source('raw', 'RAW_TSPR_BUS_LINE_2019') }}
),
arcgis_2022 as (
    select *, '2022' as snapshot_year from {{ source('raw', 'RAW_TSPR_BUS_LINE_2022') }}
),
arcgis_2023 as (
    select *, '2023' as snapshot_year from {{ source('raw', 'RAW_TSPR_BUS_LINE_2023') }}
),
arcgis_2024 as (
    select *, '2024' as snapshot_year from {{ source('raw', 'RAW_TSPR_BUS_LINE_2024') }}
),

unioned as (
    select * from arcgis_2019
    union all
    select * from arcgis_2022
    union all
    select * from arcgis_2023
    union all
    select * from arcgis_2024
),

-- Strip unit suffixes before numeric casts.
-- ArcGIS exports values like "85 km/h", "12.3%", "1,234 km" — we remove
-- everything that isn't a digit, decimal point, or leading minus sign.
stripped as (
    select
        snapshot_year,

        -- Route identifiers
        trim("Line")                         as line_raw,
        trim("Line_Name")                    as line_name,
        trim("Sub_Region_of_Primary_Service") as subregion_raw,
        trim("Predominant_Vehicle_Type")     as vehicle_type,
        trim("TSG_Service_Type")             as service_type,
        trim("NOTE")                         as note,
        trim("Exchange_Connections")         as exchange_connections,
        trim("Rail_Connections")             as rail_connections,
        trim("TSPR_Year")                    as tspr_year_raw,

        -- Strip non-numeric chars then cast (handles "1,234 km" → "1234")
        regexp_replace("Population",                    '[^0-9.-]', '')  as population_str,
        regexp_replace("Employment",                    '[^0-9.-]', '')  as employment_str,
        regexp_replace("Average_One_Way_Trip_Dist_km",  '[^0-9.-]', '')  as avg_trip_dist_km_str,
        regexp_replace("AVG_speed_km_per_hr",           '[^0-9.-]', '')  as avg_speed_km_per_hr_str,
        regexp_replace("RANK_Annual_Boardings",         '[^0-9.-]', '')  as rank_annual_boardings_str,
        regexp_replace("Annual_Boardings",              '[^0-9.-]', '')  as annual_boardings_str,
        regexp_replace("AVG_Daily_Boardings_MF",        '[^0-9.-]', '')  as avg_daily_boardings_mf_str,
        regexp_replace("AVG_Daily_Boardings_Sat",       '[^0-9.-]', '')  as avg_daily_boardings_sat_str,
        regexp_replace("AVG_Daily_Boardings_SunHol",    '[^0-9.-]', '')  as avg_daily_boardings_sunhol_str,
        regexp_replace("Annual_Revenue_Hours_with_Overc",'[^0-9.-]', '') as annual_revenue_hrs_overcrowding_str,
        regexp_replace("On_Time_Performance_Percentage",'[^0-9.-]', '')  as on_time_pct_str,
        regexp_replace("Annual_Service_Cost",           '[^0-9.-]', '')  as annual_service_cost_str,
        regexp_replace("Annual_Service_Hours",          '[^0-9.-]', '')  as annual_service_hours_str,
        regexp_replace("SHAPE__Length",                 '[^0-9.-]', '')  as shape_length_str

    from unioned
    -- Exclude non-bus services that appear in the ArcGIS export
    where "Line" is not null
      and upper(trim("Line")) not in ('SEABUS', 'WEST COAST EXPRESS', 'WCE')
),

casted as (
    select
        try_to_number(snapshot_year)                    as snapshot_year,

        -- Route
        {{ parse_combined_route('line_raw') }}           as route_number,
        line_raw,
        line_name,
        vehicle_type,
        service_type,
        note,
        exchange_connections,
        rail_connections,
        try_to_number(tspr_year_raw)                    as tspr_year,

        -- Sub-region — standardise to match subregion_population seed.
        -- Note: ArcGIS data uses different names across years (e.g. "Southeast"
        -- in 2019 vs "Southeast (Surrey/Langley)" in later years, and
        -- "Maple Ridge/Pitt Meadows" vs "Ridge Meadows"). Southwest
        -- (Richmond/Delta) appears as a distinct region in all years.
        case
            when upper(subregion_raw) like '%VANCOUVER%' or upper(subregion_raw) like '%UBC%'
                then 'Vancouver/UBC'
            when upper(subregion_raw) like '%BURNABY%' or upper(subregion_raw) like '%NEW WEST%'
                then 'Burnaby/New Westminster'
            when upper(subregion_raw) like '%NORTH SHORE%'
                then 'North Shore'
            when upper(subregion_raw) like '%NORTHEAST%'
                then 'Northeast Sector'
            -- "Southeast" (raw) and "Southeast (Surrey/Langley)" both map here
            when upper(subregion_raw) like '%SOUTHEAST%'
                or upper(subregion_raw) like '%SURREY%'
                or upper(subregion_raw) like '%LANGLEY%'
                then 'Southeast (Surrey/Langley)'
            -- "Southwest" / "Richmond" / "Delta" → Southwest (Richmond/Delta)
            when upper(subregion_raw) like '%SOUTHWEST%'
                or upper(subregion_raw) like '%RICHMOND%'
                or upper(subregion_raw) like '%DELTA%'
                then 'Southwest (Richmond/Delta)'
            when upper(subregion_raw) like '%RIDGE%'
                or upper(subregion_raw) like '%MEADOW%'
                or upper(subregion_raw) like '%MAPLE RIDGE%'
                or upper(subregion_raw) like '%PITT MEADOW%'
                then 'Ridge Meadows'
            when upper(subregion_raw) like '%TRI%'
                or upper(subregion_raw) like '%COQUITLAM%'
                or upper(subregion_raw) like '%PORT MOODY%'
                then 'Tri-Cities'
            else subregion_raw  -- keep original if unmatched (will surface in tests)
        end                                             as subregion,
        subregion_raw,

        -- Demographics (route corridor population/employment from ArcGIS)
        try_to_number(population_str, 18, 0)            as corridor_population,
        try_to_number(employment_str, 18, 0)            as corridor_employment,

        -- Route geometry
        try_to_number(avg_trip_dist_km_str, 10, 2)      as avg_trip_dist_km,
        try_to_number(shape_length_str, 18, 4)          as shape_length,

        -- Performance
        try_to_number(avg_speed_km_per_hr_str, 10, 2)   as avg_speed_km_per_hr,
        try_to_number(rank_annual_boardings_str, 10, 0)  as rank_annual_boardings,
        try_to_number(annual_boardings_str, 18, 3)       as annual_boardings,
        try_to_number(avg_daily_boardings_mf_str, 10, 1) as avg_daily_boardings_mf,
        try_to_number(avg_daily_boardings_sat_str, 10, 1) as avg_daily_boardings_sat,
        try_to_number(avg_daily_boardings_sunhol_str, 10, 1) as avg_daily_boardings_sunhol,
        try_to_number(annual_revenue_hrs_overcrowding_str, 10, 2) as annual_revenue_hrs_overcrowding,
        try_to_number(on_time_pct_str, 10, 4)           as on_time_performance_pct,
        try_to_number(annual_service_cost_str, 18, 2)   as annual_service_cost,
        try_to_number(annual_service_hours_str, 10, 2)  as annual_service_hours

    from stripped
)

select * from casted
