"""Status refresh -- drawio node `stat` ("keeps map dots live"), edge `e26`.

The forecast cube is regenerated hourly; the dots on the map are current
occupancy and have to move faster than that. This writes ONE DynamoDB item
holding every station's live counts, so the web tool's map render is a single
GetItem rather than ~250 of them.

Deliberately separate from the inference Lambda. Inference loads a model
bundle, calls three upstreams and writes ~12 K rows; this calls one upstream
and writes one item. Fusing them would tie a 60-second cadence to a
minute-long job.

This is a display path only. It never feeds training -- the polled archive
that DOES feed validation is written by the poller Lambda to /raw
(pipelines.md 0.5), and that is a different function writing a different store.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

from lakeio import LOG, fetch

DDB = boto3.resource("dynamodb")

TABLE_NAME = os.environ["TABLE_NAME"]
TTL_MINUTES = int(os.environ.get("TTL_MINUTES", "60"))
GBFS_STATION_STATUS = os.environ.get(
    "STATION_STATUS_URL", "https://gbfs.bcycle.com/bcycle_indego/station_status.json"
)


def handler(event, context):
    now = datetime.now(timezone.utc)
    payload = json.loads(fetch(GBFS_STATION_STATUS, timeout=20))
    stations = payload.get("data", {}).get("stations", [])

    if not stations:
        raise RuntimeError("station_status returned no stations")

    snapshot = {}
    for s in stations:
        try:
            station_id = int(s["station_id"])
        except (KeyError, ValueError, TypeError):
            continue

        bikes = int(s.get("num_bikes_available") or 0)
        docks = int(s.get("num_docks_available") or 0)
        capacity = bikes + docks

        snapshot[str(station_id)] = {
            "bikes": bikes,
            "docks": docks,
            "capacity": capacity,
            # pct_full is precomputed so the web tool colours a dot without
            # doing arithmetic on ~250 stations in the browser.
            "pct_full": Decimal(str(round(bikes / capacity, 4))) if capacity else Decimal("0"),
            "is_empty": bikes == 0,
            "is_full": docks == 0,
            "is_renting": bool(s.get("is_renting", 1)),
            "is_returning": bool(s.get("is_returning", 1)),
        }

    table = DDB.Table(TABLE_NAME)
    table.put_item(Item={
        "pk": "SNAPSHOT#CURRENT",
        "sk": "ALL",
        "polled_at": now.isoformat(),
        "feed_last_updated": payload.get("last_updated"),
        "station_count": len(snapshot),
        "stations": snapshot,
        # A TTL well past the refresh interval. Its job is to make a dead
        # refresher visible as an absent item rather than as indefinitely
        # stale numbers presented as live.
        "expires_at": int((now + timedelta(minutes=TTL_MINUTES)).timestamp()),
    })

    LOG.info("refreshed %s stations", len(snapshot))
    return {"stations": len(snapshot), "polled_at": now.isoformat()}
