/*
  assert_valid_year.sql

  Fails if stg_bus_performance contains calendar_year values outside the
  known TSPR coverage window (2019–2024). Catches load errors where a
  report file contains unexpected years (e.g. a future report is added
  without updating this model).
*/

select
    route_number,
    calendar_year
from {{ ref('stg_bus_performance') }}
where calendar_year is null
   or calendar_year not in (2019, 2020, 2021, 2022, 2023, 2024)
