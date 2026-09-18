"""station_status poller -- drawio node `g3`, pipelines.md step 0.5.

THE highest-urgency collector. There is no published historical archive of
dock-level occupancy; GBFS station_status is a live snapshot only. Every day
this does not run is validation data that can never be recovered.

It gates nothing in the current delivery -- step 6.6 is deferred past the
delivery date (.claude/decisions.md) -- which is exactly why it is easy to
forget. It runs from day one anyway.

Writes straight to S3 rather than through Kinesis Firehose: the Lambda already
holds ~250 records in memory per interval, and Firehose would add a service,
~105 K small objects/year, and a compaction job for no benefit at this volume
(.claude/decisions.md).
"""

from __future__ import annotations

import json
import os

from lakeio import LOG, fetch, put_json_gz, utcnow

RAW_BUCKET = os.environ["RAW_BUCKET"]
GBFS_STATION_STATUS = os.environ.get(
    "STATION_STATUS_URL", "https://gbfs.bcycle.com/bcycle_indego/station_status.json"
)


def handler(event, context):
    now = utcnow()
    payload = json.loads(fetch(GBFS_STATION_STATUS, timeout=30))
    stations = payload.get("data", {}).get("stations", [])

    if not stations:
        # Fail loudly. A silently empty poll looks identical to a healthy one
        # in the object listing, and the gap only surfaces months later when
        # the validation set is built.
        raise RuntimeError("station_status returned no stations")

    # Hive-style partitioning on the UTC hour. Step 1.9 reads this with a
    # partition predicate rather than listing ~105 K objects, and the poll
    # instant is in the key so the ordering is recoverable from the listing
    # alone.
    key = (
        f"raw/station_status/dt={now:%Y-%m-%d}/hour={now:%H}/"
        f"station_status_{now:%Y%m%dT%H%M%SZ}.json.gz"
    )

    record = {
        # last_updated is the feed's own clock. Both are kept: a divergence
        # between them is how a stale feed is detected, and only the feed's
        # clock is meaningful for joining against trip timestamps.
        "polled_at": now.isoformat(),
        "feed_last_updated": payload.get("last_updated"),
        "ttl": payload.get("ttl"),
        "stations": stations,
    }

    put_json_gz(RAW_BUCKET, key, record)
    LOG.info("polled %s stations -> %s", len(stations), key)

    return {"stations": len(stations), "key": key}
