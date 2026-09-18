"""Shared helpers for the collector Lambdas.

Deliberately standard library plus boto3 only. boto3 ships in the Lambda
runtime, so there is no build step, no layer to pin, and no wheel to rebuild
when the runtime moves. The cost of that choice is that nothing here writes
Parquet -- which is correct: /raw is "land as dated JSON" (pipelines.md 0.5)
and the raw -> Parquet conversion is step 1.9, a Spark job.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

LOG = logging.getLogger()
LOG.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

S3 = boto3.client("s3")

# Indego and PASDA both sit behind CDNs that reject the default urllib agent.
USER_AGENT = "station-balance/1.0 (+https://github.com/; research pipeline)"

DEFAULT_TIMEOUT_S = 60


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def fetch(url: str, *, timeout: int = DEFAULT_TIMEOUT_S, retries: int = 4,
          data: bytes | None = None, headers: dict | None = None) -> bytes:
    """GET (or POST, if `data` is given) with bounded exponential backoff.

    Every upstream here is a free public endpoint with no SLA -- PGW in
    particular runs on IIS 10 / ASP.NET 2.0.50727 (.claude/decisions.md). Retry
    is not optional, but neither is giving up: a Lambda that retries forever
    burns its 15-minute budget and reports success for a partial fetch.
    """
    req_headers = {"User-Agent": USER_AGENT}
    if headers:
        req_headers.update(headers)

    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=req_headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            # 404 means the resource genuinely is not there (a quarter that has
            # not been published yet). Retrying cannot fix it.
            if isinstance(exc, urllib.error.HTTPError) and exc.code == 404:
                raise
            backoff = 2 ** attempt
            LOG.warning("fetch failed (%s/%s) %s: %s -- retrying in %ss",
                        attempt + 1, retries, url, exc, backoff)
            time.sleep(backoff)

    raise RuntimeError(f"fetch failed after {retries} attempts: {url}") from last_error


def exists(bucket: str, key: str) -> bool:
    try:
        S3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def put_bytes(bucket: str, key: str, body: bytes, content_type: str) -> None:
    S3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    LOG.info("wrote s3://%s/%s (%s bytes)", bucket, key, len(body))


def put_json_gz(bucket: str, key: str, payload) -> None:
    """Land a JSON document gzipped.

    The poller writes ~288 objects/day and station_status is ~250 highly
    repetitive records, so gzip is roughly a 10x saving on both storage and
    the bytes Athena later scans. `.json.gz` is transparently readable by
    Spark, so step 1.9 needs no special case.
    """
    body = gzip.compress(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    S3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/gzip")
    LOG.info("wrote s3://%s/%s (%s bytes gzipped)", bucket, key, len(body))


def land_once(bucket: str, key: str, url: str, content_type: str) -> str:
    """Fetch `url` into `key` unless `key` already exists.

    /raw is immutable: "download once, never re-fetch, so results reproduce
    even if Indego revises a file" (pipelines.md 1.1). Skipping on existence is
    what makes the ingest Lambda safe to re-invoke, which matters because it is
    driven by attended scripts that get re-run.
    """
    if exists(bucket, key):
        LOG.info("skip (already landed) s3://%s/%s", bucket, key)
        return "skipped"

    put_bytes(bucket, key, fetch(url), content_type)
    return "landed"
