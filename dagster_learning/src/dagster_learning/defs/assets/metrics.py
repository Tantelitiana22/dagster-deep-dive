from datetime import datetime, timedelta
from pathlib import Path

import dagster as dg

from dagster_duckdb import DuckDBResource
from dagster_learning.defs.assets import constants
import matplotlib.pyplot as plt
import geopandas as gpd
import pandas as pd


@dg.asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource) -> None:
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by zone, borough, geometry
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()
    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)


    manhattan_path = Path(constants.MANHATTAN_STATS_FILE_PATH)
    
    manhattan_path.write_text(trips_by_zone.to_json())



@dg.asset(deps=["taxi_trips"])
def trips_by_week(database: DuckDBResource) -> None:
    query = """
        WITH weeks AS (
            SELECT unnest(
                generate_series(
                    DATE '2023-03-05',
                    DATE '2023-03-26',
                    INTERVAL '1 week'
                )
            )::DATE AS period
        )

        SELECT
            w.period,
            COUNT(t.vendor_id)::INTEGER AS num_trips,
            ROUND(COALESCE(SUM(t.total_amount), 0), 2) AS total_amount,
            ROUND(COALESCE(SUM(t.trip_distance), 0), 2) AS trip_distance,
            COALESCE(SUM(t.passenger_count), 0)::INTEGER AS passenger_count
        FROM weeks w
        LEFT JOIN trips t
            ON t.pickup_datetime >= w.period
            AND t.pickup_datetime < w.period + INTERVAL '1 week'
        GROUP BY w.period
        ORDER BY w.period
    """

    with database.get_connection() as conn:
        result = conn.execute(query).fetch_df()

    result.to_csv(constants.TRIPS_BY_WEEK_FILE_PATH, index=False)

@dg.asset(
    deps=["manhattan_stats"],
)
def manhattan_map() -> None:
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig, ax = plt.subplots(figsize=(10, 10))
    trips_by_zone.plot(column="num_trips", cmap="plasma", legend=True, ax=ax, edgecolor="black")
    ax.set_title("Number of Trips per Taxi Zone in Manhattan")

    ax.set_xlim(-74.05, -73.90)  # Adjust longitude range
    ax.set_ylim(40.70, 40.82)  # Adjust latitude range
    
    # Save the image
    plt.savefig(constants.MANHATTAN_MAP_FILE_PATH, format="png", bbox_inches="tight")
    plt.close(fig)
