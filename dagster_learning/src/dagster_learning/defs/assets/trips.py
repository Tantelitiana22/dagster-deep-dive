import os
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from dagster_learning.defs.assets import constants
import requests


def download_file(url: str, file_path: Path) -> None:
    """Download a file if it does not already exist."""
    if file_path.exists():
        print(f"File already exists: {file_path}")
        return

    file_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    file_path.write_bytes(response.content)
    print(f"Downloaded: {file_path}")


@dg.asset
def taxi_trips_file() -> None:
    """The raw parquet files for the taxi trips dataset."""

    month_to_fetch = "2023-03"

    file_path = Path(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)
    )

    url = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{month_to_fetch}.parquet"
    )

    download_file(url, file_path)


@dg.asset
def taxi_zones_file() -> None:
    """The raw CSV file for the taxi zones dataset."""

    file_path = Path(constants.TAXI_ZONES_FILE_PATH)

    url = (
        "https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/"
        "dagster-university/data/taxi_zones.csv"
    )

    download_file(url, file_path)


def execute_query(database: DuckDBResource,query: str, parameters: tuple = ()) -> None:
    """Execute a SQL query against DuckDB."""
    with database.get_connection() as conn:
        conn.execute(query,parameters)

@dg.asset(deps=["taxi_trips_file"])
def taxi_trips(database: DuckDBResource) -> None:
    """Load the raw taxi trips dataset into DuckDB."""

    query = """
        CREATE OR REPLACE TABLE trips AS
        SELECT
            VendorID AS vendor_id,
            PULocationID AS pickup_zone_id,
            DOLocationID AS dropoff_zone_id,
            RatecodeID AS rate_code_id,
            payment_type,
            tpep_dropoff_datetime AS dropoff_datetime,
            tpep_pickup_datetime AS pickup_datetime,
            trip_distance,
            passenger_count,
            total_amount
        FROM read_parquet(?)
    """

    file_path = "data/raw/taxi_trips_2023-03.parquet"

    execute_query(database, query, (file_path,))


@dg.asset(deps=["taxi_zones_file"])
def taxi_zones(database: DuckDBResource) -> None:
    """Load the raw taxi zones dataset into DuckDB."""

    query = """
        CREATE OR REPLACE TABLE zones AS
        SELECT
            LocationID AS zone_id,
            zone,
            borough,
            the_geom AS geometry
        FROM read_csv_auto(?)
    """

    file_path = str(constants.TAXI_ZONES_FILE_PATH)

    execute_query(database,query, (file_path,))