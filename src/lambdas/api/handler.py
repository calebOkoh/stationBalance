"""Read API behind API Gateway — serves the three routes drawio `api` names.

  GET /stations              live dock counts for every station (one GetItem)
  GET /forecast?station_id=  the precomputed P(empty) curve for one station
  GET /attribution           SHAP effect sizes per factor group

Why this function exists at all
-------------------------------
The architecture draws DynamoDB reading straight into API Gateway. An HTTP API
cannot integrate directly with DynamoDB -- AWS service integrations on
apigatewayv2 cover EventBridge, SQS, SNS, Step Functions, Kinesis and
AppConfig, and not DynamoDB. The alternatives were a REST API with a VTL
mapping template (3.5x the per-request price and an untestable template) or
this: ~100 lines of Python doing exactly the single read the diagram describes.

It holds no model and makes no prediction. "The model is not on the request
path" (README section 2) still holds: every number returned here was computed
by the inference Lambda on a schedule and written to DynamoDB.
"""

from __future__ import annotations

import decimal
import json
import os

import boto3
from boto3.dynamodb.conditions import Key

DDB = boto3.resource("dynamodb")
S3 = boto3.client("s3")

TABLE_NAME = os.environ["TABLE_NAME"]
GOLD_BUCKET = os.environ["GOLD_BUCKET"]
BUNDLE_PREFIX = os.environ.get("BUNDLE_PREFIX", "models/current")

# Matches the CloudFront TTL in front of it. A stale cube is the intended
# degradation mode, so caching it at the edge costs nothing in correctness.
CACHE_CONTROL = "public, max-age=60"


class _DecimalEncoder(json.JSONEncoder):
    """DynamoDB returns Decimal; JSON has no Decimal.

    Integral values are emitted as int so a dock count renders as `4` rather
    than `4.0`, which a map client would otherwise have to clean up.
    """

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return int(o) if o == o.to_integral_value() else float(o)
        return super().default(o)


def _response(status: int, body, cache: bool = True) -> dict:
    headers = {"content-type": "application/json"}
    if cache:
        headers["cache-control"] = CACHE_CONTROL
    return {
        "statusCode": status,
        "headers": headers,
        "body": json.dumps(body, cls=_DecimalEncoder),
    }


def get_stations(_params) -> dict:
    item = DDB.Table(TABLE_NAME).get_item(
        Key={"pk": "SNAPSHOT#CURRENT", "sk": "ALL"}
    ).get("Item")

    if not item:
        # The refresher writes with a TTL, so an absent item means it has
        # stopped rather than that there are no stations. 503 says "come back",
        # which is the truth; an empty 200 would render an empty map.
        return _response(503, {"error": "no current snapshot available"}, cache=False)

    return _response(200, {
        "polled_at": item.get("polled_at"),
        "station_count": item.get("station_count"),
        "stations": item.get("stations", {}),
    })


def get_forecast(params) -> dict:
    station_id = params.get("station_id")
    if not station_id:
        return _response(400, {"error": "station_id is required"}, cache=False)

    try:
        station_id = int(station_id)
    except ValueError:
        return _response(400, {"error": "station_id must be an integer"}, cache=False)

    result = DDB.Table(TABLE_NAME).query(
        KeyConditionExpression=(
            Key("pk").eq(f"STATION#{station_id}") & Key("sk").begins_with("FORECAST#")
        ),
        ScanIndexForward=True,
    )

    items = result.get("Items", [])
    if not items:
        return _response(404, {"error": f"no forecast for station {station_id}"},
                         cache=False)

    return _response(200, {
        "station_id": station_id,
        "generated_at": items[0].get("generated_at"),
        # This ordered list IS the capacity tendency graph the web tool draws.
        "forecast": [
            {
                "ts": i["ts"],
                "p_empty": i["p_empty"],
                "occupancy": i["occupancy"],
                "pct_full": i["pct_full"],
                "net_flow": i["net_flow"],
            }
            for i in items
        ],
    })


def get_attribution(_params) -> dict:
    """SHAP effect sizes per factor group — the research deliverable (6.7).

    Served from the artifact bundle rather than DynamoDB: it changes once per
    training run, not once per refresh, so it belongs with the model it
    describes.
    """
    try:
        body = S3.get_object(
            Bucket=GOLD_BUCKET, Key=f"{BUNDLE_PREFIX}/attribution.json"
        )["Body"].read()
    except S3.exceptions.NoSuchKey:
        return _response(404, {"error": "no attribution published yet"}, cache=False)

    return _response(200, json.loads(body))


ROUTES = {
    "GET /stations": get_stations,
    "GET /forecast": get_forecast,
    "GET /attribution": get_attribution,
}


def handler(event, context):
    route = event.get("routeKey", "")
    params = event.get("queryStringParameters") or {}

    fn = ROUTES.get(route)
    if fn is None:
        return _response(404, {"error": f"unknown route {route}"}, cache=False)

    try:
        return fn(params)
    except Exception as exc:  # noqa: BLE001
        # The detail goes to CloudWatch, not to the caller.
        print(f"ERROR handling {route}: {exc!r}")
        return _response(500, {"error": "internal error"}, cache=False)
