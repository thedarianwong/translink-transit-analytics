"""
load_to_snowflake.py

Loads TransLink TSPR CSV files and GTFS stops into Snowflake RAW schema.
Pattern: CREATE OR REPLACE TABLE + PUT + COPY INTO (idempotent, safe to re-run).

Usage:
    python ingestion/load_to_snowflake.py
    python ingestion/load_to_snowflake.py --year 2024          # single year
    python ingestion/load_to_snowflake.py --file gtfs_stops    # GTFS only
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

YEARS = [2019, 2020, 2021, 2022, 2023, 2024]

# Columns match the confirmed TSPR schema (all VARCHAR in RAW — casting in dbt)
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

BOARDINGS_COLUMNS = [
    "CalendarYear",
    "Lineno_renamed",
    "Subregion",
    "DayType",
    "AvgDailyBoardings",
    "Notes",
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
    "stop_timezone",
    "wheelchair_boarding",
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

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema="RAW",
    )


def create_raw_table(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    table_name: str,
    columns: list[str],
) -> None:
    """CREATE OR REPLACE TABLE with all VARCHAR columns (raw load, no casting)."""
    col_defs = ",\n    ".join(f'"{col}" VARCHAR' for col in columns)
    ddl = f"""
    CREATE OR REPLACE TABLE RAW.{table_name} (
        {col_defs}
    )
    """
    cursor.execute(ddl)
    log.info("Created table RAW.%s", table_name)


def load_csv_to_table(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    local_path: Path,
    table_name: str,
    skip_header: int = 1,
) -> int:
    """
    PUT a local CSV into a Snowflake internal stage, then COPY INTO the table.
    Returns the number of rows loaded.
    """
    stage_name = f"@%{table_name}"

    # PUT uploads the file to the table's internal stage
    put_sql = f"PUT 'file://{local_path.resolve()}' {stage_name} OVERWRITE = TRUE AUTO_COMPRESS = TRUE"
    cursor.execute(put_sql)
    log.info("PUT %s → %s", local_path.name, stage_name)

    # COPY INTO loads from stage into table
    copy_sql = f"""
    COPY INTO RAW.{table_name}
    FROM {stage_name}
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = {skip_header}
        NULL_IF = ('', 'NULL', 'N/A', 'n/a')
        EMPTY_FIELD_AS_NULL = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    )
    PURGE = TRUE
    """
    cursor.execute(copy_sql)

    # Row count from COPY result
    rows_loaded = sum(row[3] for row in cursor.fetchall())
    log.info("Loaded %d rows into RAW.%s", rows_loaded, table_name)
    return rows_loaded


# ---------------------------------------------------------------------------
# Load functions
# ---------------------------------------------------------------------------

def load_yearline(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    year: int,
) -> None:
    """Load one year of TSPR bus yearline data into RAW."""
    filename = f"tspr_bus_yearline_{year}.csv"
    filepath = RAW_DIR / filename

    if not filepath.exists():
        log.warning("File not found, skipping: %s", filepath)
        return

    table_name = f"RAW_TSPR_BUS_YEARLINE_{year}"
    create_raw_table(cursor, table_name, YEARLINE_COLUMNS)
    load_csv_to_table(cursor, filepath, table_name)


def load_boardings(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    year: int,
) -> None:
    """Load one year of TSPR bus boardings data into RAW."""
    filename = f"tspr_bus_boardings_{year}.csv"
    filepath = RAW_DIR / filename

    if not filepath.exists():
        log.warning("File not found, skipping: %s", filepath)
        return

    table_name = f"RAW_TSPR_BUS_BOARDINGS_{year}"
    create_raw_table(cursor, table_name, BOARDINGS_COLUMNS)
    load_csv_to_table(cursor, filepath, table_name)


def load_gtfs_stops(cursor: snowflake.connector.cursor.SnowflakeCursor) -> None:
    """Load GTFS stops.txt into RAW."""
    filepath = RAW_DIR / "gtfs" / "stops.txt"

    if not filepath.exists():
        log.warning("GTFS stops.txt not found, skipping: %s", filepath)
        return

    create_raw_table(cursor, "RAW_GTFS_STOPS", GTFS_STOPS_COLUMNS)
    load_csv_to_table(cursor, filepath, "RAW_GTFS_STOPS")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load TransLink TSPR data to Snowflake RAW")
    parser.add_argument(
        "--year",
        type=int,
        choices=YEARS,
        help="Load a single year only (default: all years)",
    )
    parser.add_argument(
        "--file",
        choices=["yearline", "boardings", "gtfs_stops", "all"],
        default="all",
        help="Which file type to load (default: all)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    years = [args.year] if args.year else YEARS

    log.info("Connecting to Snowflake...")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        if args.file in ("yearline", "all"):
            for year in years:
                load_yearline(cursor, year)

        if args.file in ("boardings", "all"):
            for year in years:
                load_boardings(cursor, year)

        if args.file in ("gtfs_stops", "all"):
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
