"""Pipeline 2 -- drawio node `inf` ("predict net_flow | integrate | P(empty)").

Runs the whole inference contract fixed by Pipeline 1 (README section 5):

  * loads the artifact bundle exported by step 6.8
  * reads live station_status for the integration constant O(s,t0)
  * reads live Open-Meteo using the IDENTICAL variable list in features.yaml
  * reads the current closure layer at the SAME buffer used in 4.4
  * derives calendar features from SHARED code with 4.6 (common/calendarfeat.py)
  * predicts net_flow forward, integrates from O(s,t0), emits calibrated
    P(empty) per station per hour

The model is not on the request path. This writes a precomputed cube -- ~250
stations x 48 hours is ~12 K rows -- to DynamoDB on a schedule, and API Gateway
reads DynamoDB. Enumerating beats serving live at this size, and a failed run
degrades to stale data rather than a 5xx (README section 2).

Live data is used ONLY here. It is never a training input.
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3

import gbdt
from calendarfeat import calendar_features
from lakeio import LOG, fetch

S3 = boto3.client("s3")
DDB = boto3.resource("dynamodb")

GOLD_BUCKET = os.environ["GOLD_BUCKET"]
BUNDLE_PREFIX = os.environ.get("BUNDLE_PREFIX", "models/current")
TABLE_NAME = os.environ["TABLE_NAME"]
HORIZON_HOURS = int(os.environ.get("HORIZON_HOURS", "48"))
TTL_DAYS = int(os.environ.get("TTL_DAYS", "7"))

GBFS_STATION_STATUS = "https://gbfs.bcycle.com/bcycle_indego/station_status.json"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"
ARCGIS_CLOSURES = (
    "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/"
    "LaneClosure_Master/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
)

EARTH_RADIUS_M = 6_371_008.8


# --------------------------------------------------------------------------
# Artifact bundle (step 6.8)
# --------------------------------------------------------------------------
class Bundle:
    """Model binaries plus everything needed to reproduce training features.

    "The inference pipeline must load exactly these. Versioned together, since
    a model and its preprocessing are one unit" (pipelines.md 6.8).
    """

    def __init__(self, prefix: str):
        self.net_flow = gbdt.load(_get_text(f"{prefix}/model_net_flow.txt"))
        self.is_empty = gbdt.load(_get_text(f"{prefix}/model_is_empty.txt"))
        self.config = json.loads(_get_text(f"{prefix}/features.json"))

        # Per-station static features (4.9) and the serve-time substitutes for
        # the lag features that need history -- both exported by phase 5 so
        # they are computed by the SAME code that built the training rows.
        context = json.loads(_get_text(f"{prefix}/serving_context.json"))
        self.static = {int(k): v for k, v in context["station_static"].items()}
        self.climatology = context["climatology"]
        self.rolling_7d = {int(k): v for k, v in context["net_flow_rolling_7d_mean"].items()}
        self.trained_at = context.get("trained_at")

    def station_climatology(self, station_id: int, hour: int, dow: int) -> float:
        """Mean net_flow by (station, hour, weekday) -- the 6.1 baseline table.

        Stands in for `net_flow_same_hour_last_week` at serve time. The true
        lag needs a week of reconstructed history that only exists after a
        Phase 3 run, and the climatology is what that lag is a noisy draw from.
        This IS an approximation and the only train/serve gap in the contract;
        it is recorded here rather than buried so 6.5's reported metrics are
        read with it in mind.
        """
        return float(self.climatology.get(f"{station_id}|{hour}|{dow}", 0.0))


def _get_text(key: str) -> str:
    return S3.get_object(Bucket=GOLD_BUCKET, Key=key)["Body"].read().decode("utf-8")


# --------------------------------------------------------------------------
# Live inputs
# --------------------------------------------------------------------------
def live_station_status() -> dict[int, dict]:
    payload = json.loads(fetch(GBFS_STATION_STATUS, timeout=30))
    stations = payload.get("data", {}).get("stations", [])
    if not stations:
        raise RuntimeError("station_status returned no stations")

    out = {}
    for s in stations:
        try:
            station_id = int(s["station_id"])
        except (KeyError, ValueError):
            continue
        out[station_id] = {
            "num_bikes_available": s.get("num_bikes_available", 0),
            "num_docks_available": s.get("num_docks_available", 0),
            "is_renting": s.get("is_renting", 1),
            "is_returning": s.get("is_returning", 1),
        }
    return out


def live_weather(config: dict, hours: int) -> dict[str, list]:
    """Forecast weather on the grid point pinned in features.yaml.

    The same variable list, in the same order, localised to the same timezone
    as 2.2/4.5. Requesting a superset here would silently reorder the vector.
    """
    point = config["weather"]["grid_points"][0]
    hourly = ",".join(config["weather"]["hourly"])
    tz = config["project"]["timezone"].replace("/", "%2F")

    url = (f"{OPEN_METEO_FORECAST}?latitude={point['latitude']}"
           f"&longitude={point['longitude']}&hourly={hourly}"
           f"&forecast_days={max(2, math.ceil(hours / 24) + 1)}&timezone={tz}")
    return json.loads(fetch(url, timeout=60)).get("hourly", {})


def live_closures() -> list[list[tuple[float, float]]]:
    """Currently-permitted closures, one vertex list per permit, as (lat, lon).

    Grouping is kept rather than flattened because the two closure features
    differ: `n_closures_nearby` counts PERMITS within the buffer, while
    `closed_bike_lane_m` is a LENGTH. Flattening to a bare point cloud makes
    the second one uncomputable.
    """
    try:
        payload = json.loads(fetch(ARCGIS_CLOSURES, timeout=60))
    except Exception as exc:  # noqa: BLE001
        # A closure-layer outage must degrade the closure arm to zero, not
        # fail the refresh: the weather and temporal arms are still valid and
        # stale predictions are worse than closure-free ones.
        LOG.warning("closure layer unavailable, closure features -> 0: %s", exc)
        return []

    permits: list[list[tuple[float, float]]] = []
    for feature in payload.get("features", []):
        vertices = _geometry_points(feature.get("geometry") or {})
        if vertices:
            permits.append(vertices)
    LOG.info("closure layer: %s permits with geometry", len(permits))
    return permits


def _geometry_points(geom: dict) -> list[tuple[float, float]]:
    coords, gtype = geom.get("coordinates"), geom.get("type")
    if coords is None:
        return []
    if gtype == "Point":
        return [(coords[1], coords[0])]

    out: list[tuple[float, float]] = []

    def walk(node):
        if not node:
            return
        if isinstance(node[0], (int, float)):
            out.append((node[1], node[0]))
            return
        for child in node:
            walk(child)

    walk(coords)
    return out


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance.

    Training buffers in EPSG:2272 (4.1). At the 150 m scale used here the two
    agree to well under a metre at Philadelphia's latitude, so this is
    equivalent for the threshold test -- unlike Web Mercator, which would
    inflate distances ~1.29x and is the mistake 4.1 exists to prevent.
    """
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


# --------------------------------------------------------------------------
# The forward pass
# --------------------------------------------------------------------------
def build_cube(bundle: Bundle, status: dict[int, dict],
               weather: dict[str, list], closures: list[tuple[float, float]]) -> list[dict]:
    config = bundle.config
    buffer_m = float(config["geo"]["station_buffer_m"])
    weather_vars = config["weather"]["hourly"]

    times = weather.get("time", [])
    if not times:
        raise RuntimeError("forecast returned no hourly times")

    # Align the forecast to the next whole hour so the first predicted step is
    # genuinely the next hour, not a partially-elapsed one.
    now_local = datetime.fromisoformat(times[0])
    rows: list[dict] = []

    for station_id, static in bundle.static.items():
        live = status.get(station_id)
        if live is None:
            # A station in the bundle but not in the live feed is either
            # decommissioned or temporarily dropped. Predicting for it would
            # publish a confident number about a station nobody can use.
            continue

        occupancy = float(live["num_bikes_available"])
        capacity = float(static.get("capacity") or
                         (live["num_bikes_available"] + live["num_docks_available"]) or 1)

        n_closures, closed_m = _closure_features(
            static.get("lat"), static.get("lon"), closures, buffer_m
        )

        prev_net_flow = 0.0
        for step in range(min(HORIZON_HOURS, len(times))):
            ts_local = datetime.fromisoformat(times[step])
            dow, hour = ts_local.weekday(), ts_local.hour

            record = {
                **calendar_features(ts_local),
                **{v: _at(weather.get(v), step) for v in weather_vars},
                "n_closures_nearby": float(n_closures),
                "closed_bike_lane_m": closed_m,
                "has_closure": 1.0 if n_closures else 0.0,
                "capacity": capacity,
                "bike_lane_density": float(static.get("bike_lane_density", 0.0)),
                "dist_to_centroid_m": float(static.get("dist_to_centroid_m", 0.0)),
                # Legitimate lag: at t0 the TRUE occupancy comes from
                # station_status, and from there it is the integrated value --
                # which is exactly what 4.7 says is available at serve time.
                "occupancy_lag_1h": occupancy,
                "net_flow_lag_1h": prev_net_flow,
                "net_flow_same_hour_last_week": bundle.station_climatology(station_id, hour, dow),
                "net_flow_rolling_7d_mean": bundle.rolling_7d.get(station_id, 0.0),
            }
            record["is_daylight"] = _daylight_flag(hour)

            vector = bundle.net_flow.vectorize(record)
            net_flow = bundle.net_flow.predict(vector)

            # Integrate, then clamp: occupancy is physically bounded and an
            # unclamped cumulative sum drifts out of range within a day.
            occupancy = min(max(occupancy + net_flow, 0.0), capacity)
            prev_net_flow = net_flow

            p_empty = bundle.is_empty.predict(bundle.is_empty.vectorize(record))

            rows.append({
                "station_id": station_id,
                "ts": ts_local.isoformat(),
                "net_flow": net_flow,
                "occupancy": occupancy,
                "pct_full": occupancy / capacity if capacity else 0.0,
                "p_empty": p_empty,
                "capacity": capacity,
                "n_closures_nearby": n_closures,
            })

    LOG.info("built cube: %s rows from %s stations over %sh",
             len(rows), len(bundle.static), HORIZON_HOURS)
    return rows


def _closure_features(lat, lon, permits, buffer_m):
    """(n_closures_nearby, closed_bike_lane_m) for one station.

    Length is summed over the segments whose endpoints both fall inside the
    buffer, which is the same quantity 4.4 measures after the Sedona join.
    A single isolated vertex inside the buffer contributes a count but no
    length -- correct, since a point closure blocks no lane metres.

    This does NOT re-apply the bike-network filter from 4.2: the live layer
    has no bike-relevance flag. The closure arm is therefore slightly noisier
    at serve time than in training, in the direction of over-reporting.
    """
    if lat is None or lon is None or not permits:
        return 0, 0.0

    count, metres = 0, 0.0
    for vertices in permits:
        inside = [haversine_m(lat, lon, v[0], v[1]) <= buffer_m for v in vertices]
        if not any(inside):
            continue
        count += 1
        for i in range(len(vertices) - 1):
            if inside[i] and inside[i + 1]:
                metres += haversine_m(*vertices[i], *vertices[i + 1])

    return count, metres


def _at(series, index):
    if not series or index >= len(series):
        return float("nan")
    value = series[index]
    return float("nan") if value is None else float(value)


def _daylight_flag(hour: int) -> float:
    # Sunrise/sunset are pinned as daily weather variables in features.yaml;
    # when the forecast omits them this is the fallback so the feature is
    # never silently absent from the vector.
    return 1.0 if 6 <= hour < 20 else 0.0


# --------------------------------------------------------------------------
# Publish
# --------------------------------------------------------------------------
def write_cube(rows: list[dict], generated_at: datetime) -> int:
    table = DDB.Table(TABLE_NAME)
    expires = int((generated_at + timedelta(days=TTL_DAYS)).timestamp())

    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item={
                "pk": f"STATION#{row['station_id']}",
                "sk": f"FORECAST#{row['ts']}",
                "station_id": row["station_id"],
                "ts": row["ts"],
                "net_flow": _dec(row["net_flow"]),
                "occupancy": _dec(row["occupancy"]),
                "pct_full": _dec(row["pct_full"]),
                "p_empty": _dec(row["p_empty"]),
                "capacity": _dec(row["capacity"]),
                "n_closures_nearby": row["n_closures_nearby"],
                "generated_at": generated_at.isoformat(),
                "expires_at": expires,
            })

    return len(rows)


def _dec(value: float) -> Decimal:
    """DynamoDB has no float type, and Decimal(float) carries binary noise.

    Six decimal places is far beyond the precision of a probability derived
    from a reconstructed ledger, and keeps item size down across ~12 K rows.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return Decimal("0")
    return Decimal(str(round(float(value), 6)))


def handler(event, context):
    generated_at = datetime.now(timezone.utc)
    bundle = Bundle(BUNDLE_PREFIX)
    LOG.info("loaded bundle trained_at=%s stations=%s",
             bundle.trained_at, len(bundle.static))

    status = live_station_status()
    weather = live_weather(bundle.config, HORIZON_HOURS)
    closures = live_closures()

    rows = build_cube(bundle, status, weather, closures)
    written = write_cube(rows, generated_at)

    return {
        "generated_at": generated_at.isoformat(),
        "stations": len({r["station_id"] for r in rows}),
        "rows": written,
        "horizon_hours": HORIZON_HOURS,
        "model_trained_at": bundle.trained_at,
    }
