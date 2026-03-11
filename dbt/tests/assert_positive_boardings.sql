/*
  assert_positive_boardings.sql

  Fails if any route has negative annual_boardings in stg_bus_performance.
  Zero boardings is theoretically possible for a route suspended mid-year
  (treated as valid), but negative values indicate a data or casting error.
*/

select
    route_number,
    calendar_year,
    annual_boardings
from {{ ref('stg_bus_performance') }}
where annual_boardings is not null
  and annual_boardings < 0
