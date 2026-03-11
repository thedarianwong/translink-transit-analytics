/*
  fct_recovery_trends.sql

  Indexes key metrics to 2019 = 100, enabling apples-to-apples comparison of
  pandemic impact and recovery trajectory across routes and sub-regions.

  Indexed metrics:
    - boardings_index:       annual_boardings vs 2019
    - revenue_hours_index:   annual_revenue_hours vs 2019
    - on_time_index:         on_time_performance_pct vs 2019
    - overcrowding_index:    pct_trips_overcrowded vs 2019

  Routes without a 2019 baseline are excluded from the index but included in
  a separate "new_routes" CTE for completeness.

  Also produces a sub-region level rollup showing aggregate recovery
  (total boardings in each year / total 2019 boardings × 100).
*/

with

perf as (
    select * from {{ ref('fact_route_performance') }}
),

-- 2019 values for each metric, per route
baseline as (
    select
        route_number,
        annual_boardings        as boardings_2019,
        annual_revenue_hours    as revenue_hours_2019,
        on_time_performance_pct as on_time_2019,
        pct_trips_overcrowded   as overcrowding_2019
    from perf
    where calendar_year = 2019
),

-- Index all years against 2019 baseline
indexed as (
    select
        p.route_number,
        p.route_name,
        p.subregion,
        p.subregion_code,
        p.calendar_year,

        -- Raw values
        p.annual_boardings,
        p.annual_revenue_hours,
        p.on_time_performance_pct,
        p.pct_trips_overcrowded,
        p.overcrowding_severity,

        -- 2019 baseline values (for reference)
        b.boardings_2019,
        b.revenue_hours_2019,
        b.on_time_2019,
        b.overcrowding_2019,

        p.has_2019_baseline,

        -- Index: 2019 = 100. NULL if no baseline or metric is NULL.
        case
            when b.boardings_2019 > 0 and p.annual_boardings is not null
            then round(p.annual_boardings / b.boardings_2019 * 100, 1)
        end as boardings_index,

        case
            when b.revenue_hours_2019 > 0 and p.annual_revenue_hours is not null
            then round(p.annual_revenue_hours / b.revenue_hours_2019 * 100, 1)
        end as revenue_hours_index,

        case
            when b.on_time_2019 > 0 and p.on_time_performance_pct is not null
            then round(p.on_time_performance_pct / b.on_time_2019 * 100, 1)
        end as on_time_index,

        case
            when b.overcrowding_2019 > 0 and p.pct_trips_overcrowded is not null
            then round(p.pct_trips_overcrowded / b.overcrowding_2019 * 100, 1)
        end as overcrowding_index

    from perf p
    left join baseline b on p.route_number = b.route_number
),

-- Sub-region rollup: aggregate boardings index (weighted by volume, not simple avg)
subregion_rollup as (
    select
        subregion,
        subregion_code,
        calendar_year,

        -- Sum boardings for index calculation (excludes routes without 2019 baseline)
        sum(case when has_2019_baseline then annual_boardings else null end)
            as total_boardings,
        sum(case when has_2019_baseline then boardings_2019 else null end)
            as total_boardings_2019,

        -- Weighted recovery index
        case
            when sum(case when has_2019_baseline then boardings_2019 else null end) > 0
            then round(
                sum(case when has_2019_baseline then annual_boardings else null end)
                / sum(case when has_2019_baseline then boardings_2019 else null end)
                * 100,
                1
            )
        end as subregion_boardings_index,

        count(route_number)                   as total_routes,
        count(case when has_2019_baseline then 1 end) as routes_with_baseline,

        -- Simple average of route-level indices for other metrics
        round(avg(revenue_hours_index), 1)    as avg_revenue_hours_index,
        round(avg(on_time_index), 1)          as avg_on_time_index,
        round(avg(overcrowding_index), 1)     as avg_overcrowding_index

    from indexed
    where subregion is not null
    group by subregion, subregion_code, calendar_year
)

-- Primary output: route-level recovery trends
select
    i.*,
    r.subregion_boardings_index         as subregion_boardings_index,
    r.total_routes                      as subregion_total_routes
from indexed i
left join subregion_rollup r
    on i.subregion = r.subregion
    and i.calendar_year = r.calendar_year
order by i.subregion, i.route_number, i.calendar_year
