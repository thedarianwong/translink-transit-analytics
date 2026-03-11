/*
  fct_service_gap.sql

  Weighted composite service gap score (0–100) per route per year.

  Formula (Option B — composite):
    gap_score = (0.5 × pct_trips_overcrowded)
              + (0.3 × LEAST(avg_peak_load_factor × 100, 150) / 150 × 100)
              + (0.2 × avg_capacity_utilization × 100)

  Component breakdown:
    - Overcrowding frequency (50%): pct_trips_overcrowded is stored as a
      percentage (0–100, e.g. 11.2 = 11.2%). Used directly, capped at 100.
    - Peak load pressure (30%): avg_peak_load_factor is passengers/capacity.
      1.0 = exactly at capacity. Capped at 1.5 (150%) to prevent extreme
      outliers from dominating. Normalised to 0–100.
    - Capacity utilisation (20%): avg_capacity_utilization is an average-trip
      utilisation ratio (0–1). Multiplied × 100 → 0–100.

  The higher the score, the greater the gap between ridership demand and
  available service capacity.

  Also produces a subregion-level rollup (avg gap score per subregion/year)
  for the geographic dashboard panel.
*/

with

perf as (
    select * from {{ ref('fact_route_performance') }}
    where
        pct_trips_overcrowded    is not null
        and avg_peak_load_factor    is not null
        and avg_capacity_utilization is not null
),

scored as (
    select
        route_number,
        route_name,
        subregion,
        subregion_code,
        calendar_year,
        overcrowding_severity,

        -- Raw inputs (for transparency)
        pct_trips_overcrowded,
        avg_peak_load_factor,
        avg_capacity_utilization,
        avg_daily_boardings_mf,
        annual_revenue_hours,

        -- Component scores (each 0–100)
        -- pct_trips_overcrowded is already a percentage (0–100); use directly.
        -- avg_peak_load_factor is a ratio (e.g. 1.2 = 120% capacity); scale to 0–100, cap at 150%.
        -- avg_capacity_utilization is a ratio (0–1); scale to 0–100.
        round(least(pct_trips_overcrowded, 100), 2)
            as component_overcrowding_freq,

        round(least(avg_peak_load_factor * 100, 150) / 150 * 100, 2)
            as component_peak_load,

        round(avg_capacity_utilization * 100, 2)
            as component_capacity_util,

        -- Composite gap score
        round(
              (0.5 * least(pct_trips_overcrowded, 100))
            + (0.3 * least(avg_peak_load_factor * 100, 150) / 150 * 100)
            + (0.2 * avg_capacity_utilization * 100),
            2
        ) as gap_score

    from perf
),

-- Subregion rollup: average gap score and component scores per subregion/year
subregion_rollup as (
    select
        subregion,
        subregion_code,
        calendar_year,
        count(route_number)                  as route_count,
        round(avg(gap_score), 2)             as avg_gap_score,
        round(avg(component_overcrowding_freq), 2) as avg_overcrowding_freq,
        round(avg(component_peak_load), 2)   as avg_peak_load_score,
        round(avg(component_capacity_util), 2) as avg_capacity_util_score,
        max(gap_score)                       as max_gap_score,
        -- Route with the highest gap score in this subregion/year
        max_by(route_number, gap_score)      as most_strained_route
    from scored
    where subregion is not null
    group by subregion, subregion_code, calendar_year
)

-- Primary output: route-level gap scores
select
    s.*,
    sr.avg_gap_score          as subregion_avg_gap_score,
    sr.route_count            as subregion_route_count
from scored s
left join subregion_rollup sr
    on s.subregion = sr.subregion
    and s.calendar_year = sr.calendar_year
order by calendar_year, gap_score desc
