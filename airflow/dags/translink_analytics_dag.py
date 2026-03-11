"""
TransLink Transit Analytics — Airflow DAG

Orchestrates the full pipeline:
  1. Check raw data files exist
  2. Load TSPR yearline + ArcGIS bus line CSVs to Snowflake RAW
  3. Load GTFS stops to Snowflake RAW
  4. dbt run — staging models
  5. dbt test — staging models (GATE: downstream blocked on failure)
  6. dbt run — mart models
  7. dbt test — mart models
  8. Notify complete

Schedule: @once for portfolio demo
# For annual production use, change to: schedule_interval='@yearly'
# or a specific cron: '0 9 1 2 *' (09:00 on Feb 1 — after TSPR publication)
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Constants — resolved from environment variables injected by docker-compose
# ---------------------------------------------------------------------------

DATA_DIR = Path(os.environ.get("TRANSLINK_DATA_DIR", "/opt/airflow/data/raw"))
DBT_DIR = Path(os.environ.get("TRANSLINK_DBT_DIR", "/opt/airflow/dbt"))
INGESTION_DIR = Path(os.environ.get("TRANSLINK_INGESTION_DIR", "/opt/airflow/ingestion"))

# ---------------------------------------------------------------------------
# Default DAG args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "translink-analytics",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------


def check_raw_files() -> None:
    """
    Verify that the expected raw data files exist in DATA_DIR.
    Raises FileNotFoundError if any required file is missing.

    Required files:
      - At least one tspr_bus_yearline_*.csv
      - At least one RAW_TSPR_BUS_LINE_*.csv (ArcGIS)
      - gtfs/stops.txt

    Boardings CSVs are optional (load_to_snowflake.py handles gracefully).
    """
    missing: list[str] = []

    yearline_files = list(DATA_DIR.glob("tspr_bus_yearline_*.csv"))
    if not yearline_files:
        missing.append("tspr_bus_yearline_*.csv (at least one required)")

    bus_line_files = list(DATA_DIR.glob("TSPR_OpenData_Archive_*.csv"))
    if not bus_line_files:
        missing.append("TSPR_OpenData_Archive_*.csv (at least one required)")

    gtfs_stops = DATA_DIR / "gtfs" / "stops.csv"
    if not gtfs_stops.exists():
        # GTFS is optional — ingestion script handles missing files gracefully.
        # Log a warning but do not block the pipeline.
        print(f"[check_raw_files] WARNING: GTFS stops not found at {gtfs_stops} — stg_stops will be empty")
    else:
        print(f"[check_raw_files] GTFS stops: {gtfs_stops}")

    if missing:
        raise FileNotFoundError(
            f"Missing required raw data files:\n" + "\n".join(f"  - {f}" for f in missing)
        )

    print(f"[check_raw_files] Found {len(yearline_files)} yearline file(s):")
    for f in sorted(yearline_files):
        print(f"  {f.name}")

    print(f"[check_raw_files] Found {len(bus_line_files)} bus line file(s):")
    for f in sorted(bus_line_files):
        print(f"  {f.name}")

    print(f"[check_raw_files] GTFS stops: {gtfs_stops}")


def run_ingestion(source: str) -> None:
    """
    Call load_to_snowflake.py for a given source type.

    Args:
        source: One of 'yearline', 'arcgis', 'gtfs', 'all'.
                Passed as --source argument to the ingestion script.
    """
    script = INGESTION_DIR / "load_to_snowflake.py"
    cmd = [sys.executable, str(script), "--source", source]

    print(f"[run_ingestion] Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(INGESTION_DIR),
    )

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Ingestion script failed for source='{source}' "
            f"(exit code {result.returncode})"
        )

    print(f"[run_ingestion] Completed successfully for source='{source}'")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="translink_analytics",
    description="TransLink TSPR pipeline: ingest → dbt staging → dbt marts",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",  # @yearly for production
    catchup=False,
    tags=["translink", "dbt", "snowflake"],
) as dag:

    # ------------------------------------------------------------------
    # Task 1: Verify raw files are present before doing anything
    # ------------------------------------------------------------------
    t_check_files = PythonOperator(
        task_id="download_check",
        python_callable=check_raw_files,
        doc_md="""
        **download_check** — Verify raw data files exist in DATA_DIR.
        Blocks the entire pipeline if any required file is missing.
        This prevents partial loads from silently producing bad output.
        """,
    )

    # ------------------------------------------------------------------
    # Task 2a: Load TSPR yearline CSVs (2019–2024 report files)
    # ------------------------------------------------------------------
    t_load_tspr = PythonOperator(
        task_id="load_tspr_to_snowflake",
        python_callable=run_ingestion,
        op_kwargs={"source": "yearline"},
        doc_md="""
        **load_tspr_to_snowflake** — Loads TSPR yearline CSVs into Snowflake RAW schema.
        Uses PUT + COPY INTO (idempotent). All columns loaded as VARCHAR.
        """,
    )

    # ------------------------------------------------------------------
    # Task 2b: Load ArcGIS bus line CSVs (subregion + route metadata)
    # ------------------------------------------------------------------
    t_load_arcgis = PythonOperator(
        task_id="load_arcgis_to_snowflake",
        python_callable=run_ingestion,
        op_kwargs={"source": "arcgis"},
        doc_md="""
        **load_arcgis_to_snowflake** — Loads ArcGIS Bus Line with Connections CSVs
        into RAW_TSPR_BUS_LINE_{year} tables. Provides subregion + route metadata.
        Runs in parallel with yearline and GTFS loads.
        """,
    )

    # ------------------------------------------------------------------
    # Task 2c: Load GTFS stops
    # ------------------------------------------------------------------
    t_load_gtfs = PythonOperator(
        task_id="load_gtfs_to_snowflake",
        python_callable=run_ingestion,
        op_kwargs={"source": "gtfs"},
        doc_md="""
        **load_gtfs_to_snowflake** — Loads GTFS stops.txt into RAW_GTFS_STOPS.
        Runs in parallel with TSPR and ArcGIS loads (no dependency between them).
        """,
    )

    # ------------------------------------------------------------------
    # Task 3: dbt seed (subregion_population.csv → ANALYTICS.subregion_population)
    # dbt seed must run before mart models (dim_subregions depends on it)
    # ------------------------------------------------------------------
    t_dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed --full-refresh --profiles-dir .",
        doc_md="""
        **dbt_seed** — Loads static seed CSVs into Snowflake.
        subregion_population.csv → ANALYTICS.subregion_population.
        Must complete before mart models run.
        """,
    )

    # ------------------------------------------------------------------
    # Task 4: dbt run — staging models
    # ------------------------------------------------------------------
    t_dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging --profiles-dir .",
        doc_md="""
        **dbt_run_staging** — Builds staging views in TRANSLINK_ANALYTICS.STAGING:
        - stg_bus_performance (all yearline years unioned + deduplicated)
        - stg_bus_line (ArcGIS metadata, 8 subregions)
        - stg_stops (GTFS, boardable stops in Metro Vancouver bbox)
        """,
    )

    # ------------------------------------------------------------------
    # Task 5: dbt test — staging (GATE)
    # If any test fails, Airflow marks this task FAILED and all downstream
    # tasks (marts run/test) are automatically skipped. This is the
    # data quality gate — bad staging data never reaches the mart layer.
    # ------------------------------------------------------------------
    t_dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && dbt test --select staging --profiles-dir .",
        doc_md="""
        **dbt_test_staging** — Runs all staging tests (25 tests).
        **GATE TASK:** If any test fails, this task fails and downstream
        mart tasks are blocked. Data quality is enforced before mart build.
        """,
    )

    # ------------------------------------------------------------------
    # Task 6: dbt run — mart models
    # ------------------------------------------------------------------
    t_dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts --profiles-dir .",
        doc_md="""
        **dbt_run_marts** — Builds mart tables in TRANSLINK_ANALYTICS.ANALYTICS:
        - dim_routes, dim_subregions
        - fact_route_performance
        - fct_overcrowding_ranking, fct_service_gap, fct_recovery_trends
        Only runs if staging tests pass (gate enforced by Airflow dependency).
        """,
    )

    # ------------------------------------------------------------------
    # Task 7: dbt test — mart models
    # ------------------------------------------------------------------
    t_dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_DIR} && dbt test --select marts --profiles-dir .",
        doc_md="""
        **dbt_test_marts** — Runs all mart-level tests (20 tests).
        Validates uniqueness, not_null constraints, accepted_values,
        and derived flag integrity on all 6 mart models.
        """,
    )

    # ------------------------------------------------------------------
    # Task 8: Notify complete
    # Prints a summary to Airflow logs. In production, replace with
    # SlackWebhookOperator or EmailOperator.
    # ------------------------------------------------------------------
    t_notify = BashOperator(
        task_id="notify_complete",
        bash_command="""
            echo "============================================"
            echo " TransLink Analytics Pipeline — COMPLETE"
            echo "============================================"
            echo " Snowflake RAW tables: loaded"
            echo " Staging views:        built + tested"
            echo " Mart tables:          built + tested"
            echo " Timestamp: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
            echo "============================================"
        """,
        doc_md="""
        **notify_complete** — Logs a pipeline completion summary.
        In production, swap for SlackWebhookOperator or EmailOperator
        to send team notifications on success.
        """,
    )

    # ------------------------------------------------------------------
    # DAG dependency graph
    #
    #   download_check
    #       ├── load_tspr_to_snowflake ──┐
    #       ├── load_arcgis_to_snowflake ┤
    #       └── load_gtfs_to_snowflake ──┤
    #                                    └── dbt_seed
    #                                            │
    #                                    dbt_run_staging
    #                                            │
    #                                    dbt_test_staging  ← GATE
    #                                            │
    #                                    dbt_run_marts
    #                                            │
    #                                    dbt_test_marts
    #                                            │
    #                                    notify_complete
    # ------------------------------------------------------------------

    t_check_files >> [t_load_tspr, t_load_arcgis, t_load_gtfs] >> t_dbt_seed
    t_dbt_seed >> t_dbt_run_staging >> t_dbt_test_staging
    t_dbt_test_staging >> t_dbt_run_marts >> t_dbt_test_marts >> t_notify
