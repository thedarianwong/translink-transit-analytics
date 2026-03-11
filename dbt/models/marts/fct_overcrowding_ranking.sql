/*
  fct_overcrowding_ranking.sql

  Ranks every route by pct_trips_overcrowded within each calendar year.
  Flags routes that appear in the top 20 across 3 or more years as
  "persistently overcrowded" — the chronic problem routes.

  Available years: 2019, 2022, 2023, 2024.
*/

with

perf as (
    select * from {{ ref('fact_route_performance') }}
    where pct_trips_overcrowded is not null
),

-- Rank routes by overcrowding within each year (highest = rank 1)
ranked as (
    select
        route_number,
        route_name,
        subregion,
        calendar_year,
        pct_trips_overcrowded,
        avg_peak_load_factor,
        avg_daily_boardings_mf,
        annual_revenue_hours,
        overcrowding_severity,
        row_number() over (
            partition by calendar_year
            order by pct_trips_overcrowded desc
        ) as overcrowding_rank
    from perf
),

-- Count how many years each route appears in the top 20
top20_year_counts as (
    select
        route_number,
        count(*) as years_in_top20
    from ranked
    where overcrowding_rank <= 20
    group by route_number
),

final as (
    select
        r.route_number,
        r.route_name,
        r.subregion,
        r.calendar_year,
        r.pct_trips_overcrowded,
        -- Express as a percentage for readability
        round(r.pct_trips_overcrowded * 100, 2)     as pct_trips_overcrowded_display,
        r.avg_peak_load_factor,
        r.avg_daily_boardings_mf,
        r.annual_revenue_hours,
        r.overcrowding_severity,
        r.overcrowding_rank,
        r.overcrowding_rank <= 20                   as is_top20,
        coalesce(t.years_in_top20, 0)               as years_in_top20,
        -- Persistent = in top 20 for 3+ of the 4 available years
        coalesce(t.years_in_top20, 0) >= 3          as is_persistent_overcrowded
    from ranked r
    left join top20_year_counts t on r.route_number = t.route_number
)

select * from final
order by calendar_year, overcrowding_rank
