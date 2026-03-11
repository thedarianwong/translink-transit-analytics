/*
  stg_bus_performance.sql

  Unions the two TSPR yearline report files, renames columns to snake_case,
  casts all fields to appropriate types, and adds derived/flag columns.

  Source tables and the calendar years they contain:
    RAW_TSPR_BUS_YEARLINE_2023  →  CalendarYear in (2019, 2022, 2023)
    RAW_TSPR_BUS_YEARLINE_2024  →  CalendarYear in (2022, 2023, 2024)

  OVERLAP WARNING: 2022 and 2023 data appears in both source files.
  Before relying on this model, run the verification query in Snowflake:

      select
          a."CalendarYear", a."Lineno_renamed",
          a."AnnualBoardings" as boardings_2023_report,
          b."AnnualBoardings" as boardings_2024_report
      from raw.raw_tspr_bus_yearline_2023 a
      join raw.raw_tspr_bus_yearline_2024 b
        on a."CalendarYear" = b."CalendarYear"
       and a."Lineno_renamed" = b."Lineno_renamed"
      where a."CalendarYear" in ('2022', '2023')
        and a."AnnualBoardings" <> b."AnnualBoardings";

  VERIFIED 2026-03-10: The query returns rows for nearly every route in 2022
  and 2023. Differences are sub-1% (typical range: 0.01%–0.28%), with both
  positive and negative revisions. This is standard transit reporting practice:
  Compass Card and operator data is reconciled after year-end, and the newer
  report contains revised/finalized counts. The 2024 report is authoritative
  for overlapping years.

  Dedup strategy:
    - For years unique to one report (2019 → 2023 report, 2024 → 2024 report):
      take directly.
    - For overlapping years (2022, 2023): prefer the 2024 report row
      (row_num = 1 after ordering by report_source desc).
*/

with

yearline_2023 as (
    select
        "CalendarYear"                      as calendar_year_raw,
        "Lineno_renamed"                    as route_number_raw,
        "AnnualBoardings"                   as annual_boardings_raw,
        "AVG_Daily_Boardings_MF"            as avg_daily_boardings_mf_raw,
        "AVG_Daily_Boardings_Sat"           as avg_daily_boardings_sat_raw,
        "AVG_Daily_Boardings_SunHol"        as avg_daily_boardings_sunhol_raw,
        "Annual_Revenue_Hours"              as annual_revenue_hours_raw,
        "Annual_Service_Hours"              as annual_service_hours_raw,
        "Average_Boarding_Per_Revenue_Hour" as avg_boarding_per_revenue_hour_raw,
        "Average_Peak_Passenger_Load"       as avg_peak_passenger_load_raw,
        "Average_Peak_Load_Factor"          as avg_peak_load_factor_raw,
        "Average_Capacity_Utilization"      as avg_capacity_utilization_raw,
        "Revenue_Hrs_w_Overcrowding"        as revenue_hrs_w_overcrowding_raw,
        "Perc_Trips_w_Overcrowding"         as pct_trips_overcrowded_raw,
        "On_Time_Performance_Percentage"    as on_time_performance_pct_raw,
        "Bus_Bunching_Percentage"           as bus_bunching_pct_raw,
        "AVG_speed_km_per_hr"               as avg_speed_km_per_hr_raw,
        '2023'                              as report_source
    from {{ source('raw', 'RAW_TSPR_BUS_YEARLINE_2023') }}
),

yearline_2024 as (
    select
        "CalendarYear"                      as calendar_year_raw,
        "Lineno_renamed"                    as route_number_raw,
        "AnnualBoardings"                   as annual_boardings_raw,
        "AVG_Daily_Boardings_MF"            as avg_daily_boardings_mf_raw,
        "AVG_Daily_Boardings_Sat"           as avg_daily_boardings_sat_raw,
        "AVG_Daily_Boardings_SunHol"        as avg_daily_boardings_sunhol_raw,
        "Annual_Revenue_Hours"              as annual_revenue_hours_raw,
        "Annual_Service_Hours"              as annual_service_hours_raw,
        "Average_Boarding_Per_Revenue_Hour" as avg_boarding_per_revenue_hour_raw,
        "Average_Peak_Passenger_Load"       as avg_peak_passenger_load_raw,
        "Average_Peak_Load_Factor"          as avg_peak_load_factor_raw,
        "Average_Capacity_Utilization"      as avg_capacity_utilization_raw,
        "Revenue_Hrs_w_Overcrowding"        as revenue_hrs_w_overcrowding_raw,
        "Perc_Trips_w_Overcrowding"         as pct_trips_overcrowded_raw,
        "On_Time_Performance_Percentage"    as on_time_performance_pct_raw,
        "Bus_Bunching_Percentage"           as bus_bunching_pct_raw,
        "AVG_speed_km_per_hr"               as avg_speed_km_per_hr_raw,
        '2024'                              as report_source
    from {{ source('raw', 'RAW_TSPR_BUS_YEARLINE_2024') }}
),

-- Combine both report files before deduplication
unioned as (
    select * from yearline_2023
    union all
    select * from yearline_2024
),

-- For overlapping years (2022, 2023): keep only the row from the more recent
-- report. For non-overlapping years, there is only one row so rank = 1.
deduplicated as (
    select
        *,
        row_number() over (
            partition by calendar_year_raw, route_number_raw
            order by report_source desc  -- '2024' > '2023', so 2024 report wins
        ) as _row_num
    from unioned
),

-- Cast and clean all columns
cleaned as (
    select
        -- Identifiers
        try_to_number(calendar_year_raw)                as calendar_year,
        {{ parse_combined_route('route_number_raw') }}  as route_number,

        -- Flag whether the source had a combined route entry (e.g. "005/006")
        case
            when route_number_raw like '%/%' then true
            else false
        end                                             as is_combined_route,

        -- Ridership
        try_to_number(annual_boardings_raw, 18, 3)      as annual_boardings,
        try_to_number(avg_daily_boardings_mf_raw, 10, 1)    as avg_daily_boardings_mf,
        try_to_number(avg_daily_boardings_sat_raw, 10, 1)   as avg_daily_boardings_sat,
        try_to_number(avg_daily_boardings_sunhol_raw, 10, 1) as avg_daily_boardings_sunhol,

        -- Service hours
        try_to_number(annual_revenue_hours_raw, 10, 2)  as annual_revenue_hours,
        try_to_number(annual_service_hours_raw, 10, 2)  as annual_service_hours,

        -- Efficiency
        try_to_number(avg_boarding_per_revenue_hour_raw, 10, 2) as avg_boarding_per_revenue_hour,

        -- Load metrics
        try_to_number(avg_peak_passenger_load_raw, 10, 2)   as avg_peak_passenger_load,
        try_to_number(avg_peak_load_factor_raw, 10, 4)      as avg_peak_load_factor,
        try_to_number(avg_capacity_utilization_raw, 10, 4)  as avg_capacity_utilization,

        -- Overcrowding
        try_to_number(revenue_hrs_w_overcrowding_raw, 10, 2) as revenue_hrs_w_overcrowding,
        try_to_number(pct_trips_overcrowded_raw, 10, 4)      as pct_trips_overcrowded,

        -- Reliability
        try_to_number(on_time_performance_pct_raw, 10, 4)   as on_time_performance_pct,
        try_to_number(bus_bunching_pct_raw, 10, 4)           as bus_bunching_pct,

        -- Speed
        try_to_number(avg_speed_km_per_hr_raw, 10, 2)       as avg_speed_km_per_hr,

        -- Audit
        report_source

    from deduplicated
    where _row_num = 1
      and calendar_year_raw is not null
      and route_number_raw is not null
),

-- Add derived columns
final as (
    select
        *,

        -- COVID flag — 2020 data was in neither report file, so this is
        -- a forward-looking flag for if 2020 data is added later.
        -- Currently all calendar_year values are in (2019, 2022, 2023, 2024).
        (calendar_year = 2020)              as is_covid_year,

        -- Efficiency metric: boardings per unit of service delivered
        case
            when annual_service_hours > 0
            then annual_boardings / annual_service_hours
            else null
        end                                 as boardings_per_service_hour

    from cleaned
)

select * from final
