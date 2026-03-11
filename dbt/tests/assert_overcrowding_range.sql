/*
  assert_overcrowding_range.sql

  Fails if any route has pct_trips_overcrowded outside the valid range [0, 100].
  TransLink reports this as a percentage (e.g. 11.2 means 11.2%).
  Returns rows that violate the constraint — test passes when 0 rows returned.
*/

select
    route_number,
    calendar_year,
    pct_trips_overcrowded
from {{ ref('stg_bus_performance') }}
where pct_trips_overcrowded is not null
  and (
      pct_trips_overcrowded < 0
      or pct_trips_overcrowded > 100
  )
