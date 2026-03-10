# Metabase Dashboard

## Setup

1. Start Metabase via Docker (see `airflow/docker-compose.yml` for the full stack)
2. Navigate to `http://localhost:3000`
3. Connect to Snowflake:
   - **Database type:** Snowflake
   - **Account:** your Snowflake account identifier
   - **Database:** `TRANSLINK_ANALYTICS`
   - **Schema:** `ANALYTICS`
   - **Username / Password:** your service account credentials

## Dashboard Panels

| # | Panel | Source Model | Chart Type |
|---|---|---|---|
| 1 | Overcrowding Heat Map (top 20 routes) | `fct_overcrowding_ranking` | Grouped bar |
| 2 | Service Gap by Sub-Region | `fct_service_gap` | Horizontal bar |
| 3 | Recovery Trajectory (indexed to 2019) | `fct_recovery_trends` | Multi-series line |
| 4 | Efficiency vs Overcrowding Scatter | `fact_route_performance` | Scatter |
| 5 | 2024 Investment Plan Impact | `fact_route_performance` | Before/after bar |
| 6 | Speed & Reliability Trends | `fact_route_performance` | Dual-axis line |

## Screenshots

_Screenshots will be added after dashboard is live._
