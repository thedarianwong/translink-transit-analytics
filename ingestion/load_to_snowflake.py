"""
load_to_snowflake.py

Loads TransLink TSPR data into Snowflake RAW schema.
Pattern: CREATE OR REPLACE TABLE + PUT + COPY INTO (idempotent, safe to re-run).

Data sources:
  - tspr_bus_yearline_{report_year}.csv  → RAW_TSPR_BUS_YEARLINE_{report_year}
      Each report file contains multiple calendar years (e.g. 2023 report has 2019/2022/2023).
  - TSPR_OpenData_Archive_{year}.csv     → RAW_TSPR_BUS_LINE_{year}
      ArcGIS Bus Line with Connections — subregion, route name, service type, cost.
  - data/raw/gtfs/stops.csv             → RAW_GTFS_STOPS

Usage:
    python ingestion/load_to_snowflake.py                  # load everything
    python ingestion/load_to_snowflake.py --source yearline
    python ingestion/load_to_snowflake.py --source arcgis
    python ingestion/load_to_snowflake.py --source gtfs
"""

import os
import sys
import argparse
import logging
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"

# TSPR yearline report files (each contains multiple calendar years)
YEARLINE_REPORT_FILES = [
    "tspr_bus_yearline_2023.csv",
    "tspr_bus_yearline_2024.csv",
]

# ArcGIS Bus Line with Connections — one file per year
ARCGIS_YEARS = [2019, 2022, 2023, 2024]

# All columns loaded as VARCHAR — casting and cleaning happens in dbt staging
YEARLINE_COLUMNS = [
    "CalendarYear",
    "Lineno_renamed",
    "AnnualBoardings",
    "AVG_Daily_Boardings_MF",
    "AVG_Daily_Boardings_Sat",
    "AVG_Daily_Boardings_SunHol",
    "Annual_Revenue_Hours",
    "Annual_Service_Hours",
    "Average_Boarding_Per_Revenue_Hour",
    "Average_Peak_Passenger_Load",
    "Average_Peak_Load_Factor",
    "Average_Capacity_Utilization",
    "Revenue_Hrs_w_Overcrowding",
    "Perc_Trips_w_Overcrowding",
    "On_Time_Performance_Percentage",
    "Bus_Bunching_Percentage",
    "AVG_speed_km_per_hr",
]

# ArcGIS Bus Line with Connections columns (consistent across 2019/2022/2023/2024)
ARCGIS_COLUMNS = [
    "Line",
    "Line_Name",
    "Sub_Region_of_Primary_Service",
    "Predominant_Vehicle_Type",
    "TSG_Service_Type",
    "Population",
    "Employment",
    "Average_One_Way_Trip_Dist_km",
    "AVG_speed_km_per_hr",
    "RANK_Annual_Boardings",
    "Annual_Boardings",
    "AVG_Daily_Boardings_MF",
    "AVG_Daily_Boardings_Sat",
    "AVG_Daily_Boardings_SunHol",
    "Annual_Revenue_Hours_with_Overc",
    "On_Time_Performance_Percentage",
    "Annual_Service_Cost",
    "Annual_Service_Hours",
    "NOTE",
    "Exchange_Connections",
    "Rail_Connections",
    "SHAPE__Length",
    "TSPR_Year",
    "OBJECTID",
    "SHAPE__Length_2",
    "GlobalID",
]

GTFS_STOPS_COLUMNS = [
    "stop_id",
    "stop_code",
    "stop_name",
    "stop_desc",
    "stop_lat",
    "stop_lon",
    "zone_id",
    "stop_url",
    "location_type",
    "parent_station",
    "wheelchair_boarding",
    "stop_timezone",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and return a Snowflake connection using env vars."""
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing)}")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="RAW",
        insecure_mode=True,  # bypasses OCSP cert check for local dev
    )
    # Explicitly set context in case the schema wasn't activated on connect
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {os.environ['SNOWFLAKE_DATABASE']}")
    cur.execute("USE SCHEMA RAW")
    cur.close()
    return conn


def create_raw_table(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    table_name: str,
    columns: list[str],
) -> None:
    """CREATE OR REPLACE TABLE with all VARCHAR columns (raw load, no casting)."""
    col_defs = ",\n    ".join(f'"{col}" VARCHAR' for col in columns)
    ddl = f"CREATE OR REPLACE TABLE {table_name} (\n    {col_defs}\n)"
    cursor.execute(ddl)
    log.info("Created table RAW.%s", table_name)


def load_csv_to_table(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    local_path: Path,
    table_name: str,
    skip_header: int = 1,
) -> int:
    """
    PUT local CSV to Snowflake internal stage, then COPY INTO table.
    Returns number of rows loaded.
    """
    stage_name = f"@%{table_name}"

    put_sql = (
        f"PUT 'file://{local_path.resolve()}' {stage_name} "
        f"OVERWRITE = TRUE AUTO_COMPRESS = TRUE"
    )
    cursor.execute(put_sql)
    log.info("PUT %s → %s", local_path.name, stage_name)

    copy_sql = f"""
    COPY INTO {table_name}
    FROM {stage_name}
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = {skip_header}
        NULL_IF = ('', 'NULL', 'N/A', 'n/a')
        EMPTY_FIELD_AS_NULL = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        ENCODING = 'UTF8'
    )
    PURGE = TRUE
    """
    cursor.execute(copy_sql)

    rows_loaded = sum(row[3] for row in cursor.fetchall())
    log.info("Loaded %d rows into RAW.%s", rows_loaded, table_name)
    return rows_loaded


# ---------------------------------------------------------------------------
# Load functions
# ---------------------------------------------------------------------------

def load_yearline_reports(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    """
    Load TSPR yearline report CSVs into RAW.
    Each report file covers multiple calendar years (e.g. the 2023 report
    contains CalendarYear 2019, 2022, 2023). Table name reflects report year.
    """
    for filename in YEARLINE_REPORT_FILES:
        filepath = RAW_DIR / filename
        if not filepath.exists():
            log.warning("Yearline file not found, skipping: %s", filepath)
            continue

        # Extract report year from filename: tspr_bus_yearline_2023.csv → 2023
        report_year = filename.split("_")[-1].replace(".csv", "")
        table_name = f"RAW_TSPR_BUS_YEARLINE_{report_year}"

        create_raw_table(cursor, table_name, YEARLINE_COLUMNS)
        load_csv_to_table(cursor, filepath, table_name)


def load_arcgis_bus_line(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    """
    Load ArcGIS Bus Line with Connections CSVs into RAW.
    One table per year: RAW_TSPR_BUS_LINE_{year}
    Contains subregion, route name, service type, cost, connectivity.
    """
    for year in ARCGIS_YEARS:
        filepath = RAW_DIR / f"TSPR_OpenData_Archive_{year}.csv"
        if not filepath.exists():
            log.warning("ArcGIS file not found, skipping: %s", filepath)
            continue

        table_name = f"RAW_TSPR_BUS_LINE_{year}"
        create_raw_table(cursor, table_name, ARCGIS_COLUMNS)
        load_csv_to_table(cursor, filepath, table_name)


def load_gtfs_stops(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    """Load GTFS stops.csv (from carsonyl/translink-derived-datasets) into RAW."""
    filepath = RAW_DIR / "gtfs" / "stops.csv"
    if not filepath.exists():
        log.warning("GTFS stops.csv not found, skipping: %s", filepath)
        return

    create_raw_table(cursor, "RAW_GTFS_STOPS", GTFS_STOPS_COLUMNS)
    load_csv_to_table(cursor, filepath, "RAW_GTFS_STOPS")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load TransLink TSPR data to Snowflake RAW schema"
    )
    parser.add_argument(
        "--source",
        choices=["yearline", "arcgis", "gtfs", "all"],
        default="all",
        help="Which data source to load (default: all)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    log.info("Connecting to Snowflake...")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        if args.source in ("yearline", "all"):
            load_yearline_reports(cursor)

        if args.source in ("arcgis", "all"):
            load_arcgis_bus_line(cursor)

        if args.source in ("gtfs", "all"):
            load_gtfs_stops(cursor)

        log.info("All done.")

    except Exception:
        log.exception("Load failed")
        sys.exit(1)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
