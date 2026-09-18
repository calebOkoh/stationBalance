"""Ingest Lambda -- drawio node `g1` ("archives | weather | geo").

One function, dispatched by a `task` key on the event, rather than one Lambda
per source. The diagram draws a single ingest Lambda and the sources share all
their machinery (retry, immutability, landing conventions); splitting them
would add functions the architecture does not contain.

Tasks:
  trips         1.1  quarterly trip archives     -> raw/trips/
  stations      1.2  station table CSV           -> raw/stations/
  station_info  1.3  GBFS station_information    -> raw/gbfs_station_info/{date}/   [daily]
  closures_bulk 1.4  ArcGIS bulk current state   -> raw/closures/bulk/
  closures      1.5  ArcGIS daily snapshot       -> raw/closures/{date}/            [daily]
  geo           1.7  PASDA reference layers      -> raw/geo/
  weather       1.8  Open-Meteo historical       -> raw/weather/

1.6 (the PGW SOAP backfill) is deliberately NOT here: it is a genuinely
one-time job over 68 quarters, so it runs from scripts/pgw_backfill.py rather
than as deployed compute (.claude/decisions.md).
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta

from lakeio import LOG, exists, fetch, land_once, put_bytes, put_json_gz, utcnow

RAW_BUCKET = os.environ["RAW_BUCKET"]

GBFS_STATION_INFO = "https://gbfs.bcycle.com/bcycle_indego/station_information.json"
INDEGO_DATA_PAGE = "https://www.rideindego.com/about/data/"

ARCGIS_CLOSURES = (
    "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/"
    "LaneClosure_Master/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
)
ARCGIS_CLOSURES_BULK_CSV = (
    "https://hub.arcgis.com/api/v3/datasets/e10172ebe7964f63830505457c0d7c2a_0/"
    "downloads/data?format=csv&spatialRefId=3857&where=1%3D1"
)
ARCGIS_CLOSURES_BULK_GEOJSON = (
    "https://hub.arcgis.com/api/v3/datasets/e10172ebe7964f63830505457c0d7c2a_0/"
    "downloads/data?format=geojson&spatialRefId=4326&where=1%3D1"
)
# The ~203 PGW permits that carry true coordinates. Not a history source -- its
# value is as a permitnumber -> coordinate crosswalk to validate the address
# resolver in step 2.6 before trusting the other ~108 K.
ARCGIS_EUN_XY = (
    "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/"
    "LaneClosure_EUN_XY/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
)

PASDA_LAYERS = {
    "bike_network": (
        "https://mapservices.pasda.psu.edu/server/rest/services/pasda/"
        "PhiladelphiaBikeNetwork_SupportingDatasets/MapServer/0/query"
    ),
    "street_centerlines": (
        "https://mapservices.pasda.psu.edu/server/rest/services/pasda/"
        "CityPhillyStreets/MapServer/18/query"
    ),
}

OPEN_METEO_HISTORICAL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

# Trip archives are published as indego-trips-{YYYY}-q{N}.zip under a
# /wp-content/uploads/{YYYY}/{MM}/ path whose month varies by release. Rather
# than guess the month, scrape the data page for the real hrefs (the CSV
# inventory flags the filename as unstable for exactly this reason).
TRIP_ZIP_RE = re.compile(
    r'href="(https://www\.rideindego\.com/wp-content/uploads/[^"]*?'
    r'indego-trips-(\d{4})-q(\d)\.zip)"',
    re.IGNORECASE,
)
STATION_CSV_RE = re.compile(
    r'href="(https://www\.rideindego\.com/wp-content/uploads/[^"]*?'
    r'indego-stations-[\d-]+\.csv)"',
    re.IGNORECASE,
)


# --------------------------------------------------------------------------
# 1.1 / 1.2 -- Indego archives
# --------------------------------------------------------------------------
def _scrape_data_page() -> str:
    return fetch(INDEGO_DATA_PAGE, timeout=60).decode("utf-8", errors="replace")


def task_trips(event: dict) -> dict:
    """Land the quarterly trip archives.

    `train_window_start` (default 2022) filters to the window features.yaml
    pins: pre-2022 quarters have a drifting schema and a different demand
    regime, and the CSV inventory flags the published pre-2017-Q3 start as
    incorrect.
    """
    min_year = int(event.get("min_year", 2022))
    page = _scrape_data_page()

    found = {}
    for url, year, quarter in TRIP_ZIP_RE.findall(page):
        year = int(year)
        if year < min_year:
            continue
        found[(year, int(quarter))] = url

    if not found:
        raise RuntimeError("no trip archives matched on the Indego data page")

    results = {}
    for (year, quarter), url in sorted(found.items()):
        key = f"raw/trips/year={year}/quarter={quarter}/indego-trips-{year}-q{quarter}.zip"
        results[f"{year}q{quarter}"] = land_once(RAW_BUCKET, key, url, "application/zip")

    return {"task": "trips", "archives": len(found), "results": results}


def task_stations(event: dict) -> dict:
    """Land the station table. Supplies `go live date`, required by 3.4 so
    pre-launch stations are not scored as zero-demand."""
    page = _scrape_data_page()
    urls = STATION_CSV_RE.findall(page)
    if not urls:
        raise RuntimeError("no station CSV link found on the Indego data page")

    # The filename is date-stamped; the newest link is the current table.
    url = sorted(urls)[-1]
    key = f"raw/stations/{url.rsplit('/', 1)[-1]}"
    return {"task": "stations", "url": url,
            "result": land_once(RAW_BUCKET, key, url, "text/csv")}


# --------------------------------------------------------------------------
# 1.3 -- GBFS station_information, daily
# --------------------------------------------------------------------------
def task_station_info(event: dict) -> dict:
    """Dated daily snapshot of station_information.

    Dated deliberately: capacity changes and there is no published history of
    it, so the snapshot IS the record of when each value was observed. Step 3.7
    cross-checks its rolling-90d capacity estimate against these.
    """
    snapshot_date = event.get("date") or utcnow().date().isoformat()
    payload = json.loads(fetch(GBFS_STATION_INFO))
    key = f"raw/gbfs_station_info/dt={snapshot_date}/station_information.json.gz"
    put_json_gz(RAW_BUCKET, key, payload)

    stations = payload.get("data", {}).get("stations", [])
    return {"task": "station_info", "date": snapshot_date, "stations": len(stations)}


# --------------------------------------------------------------------------
# 1.4 / 1.5 -- closure layer
# --------------------------------------------------------------------------
def task_closures(event: dict) -> dict:
    """Dated daily snapshot of the current-state closure layer.

    The layer purges expired permits -- verified 2026-09-17, only 1/2/15
    permits survive across 2022/2023/2024. Without dated snapshots its
    historical depth erodes silently. This is the INFERENCE path source;
    training history comes from PGW (scripts/pgw_backfill.py).
    """
    snapshot_date = event.get("date") or utcnow().date().isoformat()
    payload = json.loads(fetch(ARCGIS_CLOSURES, timeout=120))
    key = f"raw/closures/dt={snapshot_date}/lane_closures.geojson.gz"
    put_json_gz(RAW_BUCKET, key, payload)

    return {"task": "closures", "date": snapshot_date,
            "features": len(payload.get("features", []))}


def task_closures_bulk(event: dict) -> dict:
    """One-off bulk pull of current state, plus the EUN_XY validation crosswalk."""
    results = {
        "csv": land_once(RAW_BUCKET, "raw/closures/bulk/lane_closure_master.csv",
                         ARCGIS_CLOSURES_BULK_CSV, "text/csv"),
        "geojson": land_once(RAW_BUCKET, "raw/closures/bulk/lane_closure_master.geojson",
                             ARCGIS_CLOSURES_BULK_GEOJSON, "application/geo+json"),
        "eun_xy": land_once(RAW_BUCKET, "raw/closures/bulk/lane_closure_eun_xy.geojson",
                            ARCGIS_EUN_XY, "application/geo+json"),
    }
    return {"task": "closures_bulk", "results": results}


# --------------------------------------------------------------------------
# 1.7 -- PASDA reference layers
# --------------------------------------------------------------------------
def task_geo(event: dict) -> dict:
    """Page the Esri REST layers out to GeoJSON.

    Esri caps a single response at `maxRecordCount` (typically 1000-2000) and
    signals truncation with `exceededTransferLimit`, so paging is mandatory --
    a single unpaged call silently returns a partial street network.
    """
    results = {}
    for name, base in PASDA_LAYERS.items():
        key = f"raw/geo/{name}.geojson"
        features, offset, page_size = [], 0, 1000

        while True:
            url = (f"{base}?where=1%3D1&outFields=*&outSR=4326&f=geojson"
                   f"&resultOffset={offset}&resultRecordCount={page_size}")
            page = json.loads(fetch(url, timeout=120))
            batch = page.get("features", [])
            features.extend(batch)

            if not page.get("exceededTransferLimit") and len(batch) < page_size:
                break
            if not batch:
                break
            offset += len(batch)
            LOG.info("%s: %s features so far", name, len(features))

        put_bytes(
            RAW_BUCKET, key,
            json.dumps({"type": "FeatureCollection", "features": features}).encode(),
            "application/geo+json",
        )
        results[name] = len(features)

    return {"task": "geo", "results": results}


# --------------------------------------------------------------------------
# 1.8 -- Open-Meteo historical backfill
# --------------------------------------------------------------------------
def task_weather(event: dict) -> dict:
    """Backfill hourly weather for the training window.

    Fetches the 2-4 grid points pinned in features.yaml, NOT 250 stations: the
    reanalysis grid is ~11 km, so nearly all stations share a cell and
    per-station calls are wasted quota returning identical data.

    Chunked by 6 months because the API caps a single response and a whole
    4-year pull in one request times out.
    """
    cfg = json.loads(os.environ["WEATHER_CONFIG"])
    start = date.fromisoformat(event.get("start_date", cfg["start_date"]))
    end = date.fromisoformat(event.get("end_date") or utcnow().date().isoformat())
    hourly = ",".join(cfg["hourly"])
    daily = ",".join(cfg["daily"])

    results = {}
    for point in cfg["grid_points"]:
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=182), end)
            key = (f"raw/weather/point={point['name']}/"
                   f"{chunk_start.isoformat()}_{chunk_end.isoformat()}.json.gz")

            if exists(RAW_BUCKET, key):
                chunk_start = chunk_end + timedelta(days=1)
                continue

            url = (f"{OPEN_METEO_HISTORICAL}?latitude={point['latitude']}"
                   f"&longitude={point['longitude']}"
                   f"&start_date={chunk_start.isoformat()}&end_date={chunk_end.isoformat()}"
                   f"&hourly={hourly}&daily={daily}"
                   # Localised here so the join in 4.5 is on the same local hour
                   # the trips were localised to in 2.2.
                   f"&timezone={cfg['timezone'].replace('/', '%2F')}")
            put_json_gz(RAW_BUCKET, key, json.loads(fetch(url, timeout=120)))
            results[key] = "landed"
            chunk_start = chunk_end + timedelta(days=1)

    return {"task": "weather", "chunks_landed": len(results)}


TASKS = {
    "trips": task_trips,
    "stations": task_stations,
    "station_info": task_station_info,
    "closures": task_closures,
    "closures_bulk": task_closures_bulk,
    "geo": task_geo,
    "weather": task_weather,
}


def handler(event, context):
    task = (event or {}).get("task")
    if task not in TASKS:
        raise ValueError(f"unknown task {task!r}; expected one of {sorted(TASKS)}")

    LOG.info("ingest task=%s event=%s", task, json.dumps(event))
    result = TASKS[task](event)
    LOG.info("ingest task=%s done: %s", task, json.dumps(result))
    return result
