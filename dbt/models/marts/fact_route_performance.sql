/*
  fact_route_performance.sql

  One row per route per calendar year. Combines all TSPR KPIs from
  stg_bus_performance with route metadata (dim_routes) and sub-region
  demographics (dim_subregions).

  Derived columns added here:
    - recovery_rate:        ridership indexed to 2019 = 100
                            NULL for routes without a 2019 baseline
    - has_2019_baseline:    boolean flag — FALSE means recovery_rate will be NULL
    - yoy_ridership_change: % change in annual boardings vs prior available year
    - overcrowding_severity: tier based on pct_trips_overcrowded
        Low      < 5%
        Medium   5–15%
        High     15–30%
        Critical ≥ 30%

  Available years: 2019, 2022, 2023, 2024 (2020/2021 not in source data).
  YoY change for 2022 therefore represents the 2019→2022 gap, not a true
  year-over-year, so interpret with caution.
*/

with

performance as (
    select * from {{ ref('stg_bus_performance') }}
),

routes as (
    select * from {{ ref('dim_routes') }}
),

subregions as (
    select * from {{ ref('dim_subregions') }}
),

-- Pull each route's 2019 boardings as the recovery baseline
baseline_2019 as (
    select
        route_number,
        annual_boardings as boardings_2019
    from performance
    where calendar_year = 2019
),

-- Year-over-year: compare to the previous year present in the data
-- (2022 compares to 2019, 2023 to 2022, 2024 to 2023)
prior_year as (
    select
        route_number,
        calendar_year,
        lag(annual_boardings) over (
            partition by route_number
            order by calendar_year
        ) as prior_year_boardings,
        lag(calendar_year) over (
            partition by route_number
            order by calendar_year
        ) as prior_calendar_year
    from performance
),

joined as (
    select
        -- Keys
        p.route_number,
        p.calendar_year,

        -- Route metadata (latest snapshot)
        r.route_name,
        r.vehicle_type,
        r.service_type,
        r.subregion,
        r.latest_snapshot_year,

        -- Sub-region demographics
        s.subregion_code,
        s.population_2021          as subregion_population,
        s.population_density_per_km2,

        -- Ridership
        p.annual_boardings,
        p.avg_daily_boardings_mf,
        p.avg_daily_boardings_sat,
        p.avg_daily_boardings_sunhol,

        -- Service hours
        p.annual_revenue_hours,
        p.annual_service_hours,

        -- Efficiency
        p.avg_boarding_per_revenue_hour,
        p.boardings_per_service_hour,

        -- Load metrics
        p.avg_peak_passenger_load,
        p.avg_peak_load_factor,
        p.avg_capacity_utilization,

        -- Overcrowding
        p.revenue_hrs_w_overcrowding,
        p.pct_trips_overcrowded,

        -- Reliability
        p.on_time_performance_pct,
        p.bus_bunching_pct,

        -- Speed
        p.avg_speed_km_per_hr,

        -- Flags
        p.is_covid_year,
        p.is_combined_route,
        p.report_source,

        -- 2019 baseline
        b.boardings_2019,
        b.boardings_2019 is not null as has_2019_baseline,

        -- Recovery rate: 2019 = 100 index
        case
            when b.boardings_2019 is not null and b.boardings_2019 > 0
            then round(p.annual_boardings / b.boardings_2019 * 100, 1)
            else null
        end as recovery_rate,

        -- YoY change: % change from previous available year
        py.prior_calendar_year,
        case
            when py.prior_year_boardings is not null and py.prior_year_boardings > 0
            then round(
                (p.annual_boardings - py.prior_year_boardings) / py.prior_year_boardings * 100,
                2
            )
            else null
        end as yoy_ridership_change_pct,

        -- Overcrowding severity tier
        -- pct_trips_overcrowded is stored as a percentage (e.g. 11.2 = 11.2%)
        case
            when p.pct_trips_overcrowded is null then 'Unknown'
            when p.pct_trips_overcrowded >= 30   then 'Critical'
            when p.pct_trips_overcrowded >= 15   then 'High'
            when p.pct_trips_overcrowded >= 5    then 'Medium'
            else 'Low'
        end as overcrowding_severity

    from performance p
    left join routes    r  on p.route_number = r.route_number
    left join baseline_2019 b on p.route_number = b.route_number
    left join prior_year py   on p.route_number = py.route_number
                              and p.calendar_year = py.calendar_year
    left join subregions s on r.subregion = s.subregion_name
)

select * from joined
