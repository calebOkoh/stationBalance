"""Ingest Lambda — downloads the historical archives this project trains on.

Three sources, three tasks, all of them one-off downloads of published archives:

  trips     1.1  quarterly trip archives  -> raw/trips/
  stations  1.2  station table CSV        -> raw/stations/
  weather   1.8  Open-Meteo historical    -> raw/weather/

That is the complete input list. Nothing here polls, and no task reads a
current-state feed: GBFS station_status, the live Open-Meteo forecast and the
ArcGIS closure layer are all live sources and belong to the inference
architecture, which is drawn in docs/live_inference.drawio and not built.

Every task is idempotent. raw/ is immutable — "download once, never re-fetch,
so results reproduce even if Indego revises a file" (pipelines.md 1.1) — so
anything already landed is skipped. That is what makes this safe to re-run,
which matters because it is driven by an attended script that gets re-run.
"""

from __future__ import annotations

import json
import os
import re
from datetime import date, timedelta

from lakeio import LOG, exists, fetch, land_once, put_json_gz, utcnow

DATA_BUCKET = os.environ["DATA_BUCKET"]

INDEGO_DATA_PAGE = "https://www.rideindego.com/about/data/"
OPEN_METEO_HISTORICAL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

# Trip archives live under /wp-content/uploads/{YYYY}/{MM}/ where the month
# varies by release date, so the data page is scraped for real hrefs rather
# than the path being guessed. The station CSV is date-stamped for the same
# reason — the source inventory flags both filenames as unstable.
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


def _scrape_data_page() -> str:
    return fetch(INDEGO_DATA_PAGE, timeout=60).decode("utf-8", errors="replace")


def task_trips(event: dict) -> dict:
    """1.1 — the quarterly trip archives.

    Filtered to the window features.yaml pins. Pre-2022 quarters have a
    drifting schema and a different demand regime, and the published
    pre-2017-Q3 start date is flagged as incorrect in the source inventory.
    """
    min_year = int(event.get("min_year", 2022))
    page = _scrape_data_page()

    found = {}
    for url, year, quarter in TRIP_ZIP_RE.findall(page):
        if int(year) >= min_year:
            found[(int(year), int(quarter))] = url

    if not found:
        raise RuntimeError("no trip archives matched on the Indego data page")

    results = {}
    for (year, quarter), url in sorted(found.items()):
        key = f"raw/trips/year={year}/quarter={quarter}/indego-trips-{year}-q{quarter}.zip"
        results[f"{year}q{quarter}"] = land_once(DATA_BUCKET, key, url, "application/zip")

    return {"task": "trips", "archives": len(found), "results": results}


def task_stations(event: dict) -> dict:
    """1.2 — the station table.

    Supplies the go-live date, which step 3.4 needs so a station is not scored
    as zero-demand for the years before it existed, plus lat/lon and the
    current dock count.
    """
    urls = STATION_CSV_RE.findall(_scrape_data_page())
    if not urls:
        raise RuntimeError("no station CSV link found on the Indego data page")

    # The filename carries its date, so the newest link is the current table.
    url = sorted(urls)[-1]
    key = f"raw/stations/{url.rsplit('/', 1)[-1]}"
    return {"task": "stations", "url": url,
            "result": land_once(DATA_BUCKET, key, url, "text/csv")}


def task_weather(event: dict) -> dict:
    """1.8 — hourly weather for the training window.

    Fetches the 2-4 grid points pinned in features.yaml, NOT 250 stations: the
    reanalysis grid is ~11 km, so nearly all stations share a cell and
    per-station calls are wasted quota returning identical data.

    Chunked by six months because a whole multi-year pull in one request times
    out, and because a failure then costs one chunk rather than everything.
    """
    cfg = json.loads(os.environ["WEATHER_CONFIG"])
    start = date.fromisoformat(event.get("start_date", cfg["start_date"]))
    end = date.fromisoformat(event.get("end_date") or utcnow().date().isoformat())
    hourly, daily = ",".join(cfg["hourly"]), ",".join(cfg["daily"])

    landed = 0
    for point in cfg["grid_points"]:
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=182), end)
            key = (f"raw/weather/point={point['name']}/"
                   f"{chunk_start.isoformat()}_{chunk_end.isoformat()}.json.gz")

            if not exists(DATA_BUCKET, key):
                url = (f"{OPEN_METEO_HISTORICAL}?latitude={point['latitude']}"
                       f"&longitude={point['longitude']}"
                       f"&start_date={chunk_start.isoformat()}"
                       f"&end_date={chunk_end.isoformat()}"
                       f"&hourly={hourly}&daily={daily}"
                       # Localised here so the join in 4.5 is on the same local
                       # hour the trips were localised to in 2.2.
                       f"&timezone={cfg['timezone'].replace('/', '%2F')}")
                payload = json.loads(fetch(url, timeout=120))
                payload["_point"] = point
                put_json_gz(DATA_BUCKET, key, payload)
                landed += 1

            chunk_start = chunk_end + timedelta(days=1)

    return {"task": "weather", "chunks_landed": landed,
            "grid_points": len(cfg["grid_points"])}


TASKS = {
    "trips": task_trips,
    "stations": task_stations,
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
