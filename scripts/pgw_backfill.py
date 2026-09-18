#!/usr/bin/env python3
"""Step 1.6 -- the one-time PGW closure-history backfill.

PGW is the ONLY real source of closure history. The Philadelphia Streets
Department layer purges expired permits, so it holds nothing usable before 2025
(verified 2026-09-17: 1 / 2 / 15 permits survive from 2022 / 2023 / 2024).
Without this pull, one of the three factors the project sets out to measure has
no training data at all.

Why this is a script and not deployed compute
---------------------------------------------
The architecture draws ECS Fargate here. .claude/decisions.md cut it: this is
"a genuinely one-time job. Run it once from a laptop or a small EC2 box, write
to S3, never run it again." Standing up a cluster for ~68 sequential HTTP POSTs
is infrastructure for its own sake.

Why no zeep
-----------
A SOAP 1.1 call is an HTTP POST with an XML envelope and a SOAPAction header.
zeep would add a dependency, and its WSDL parser is stricter than this service
-- which runs on IIS 10 / ASP.NET 2.0.50727 with no SLA or versioning. The
envelope is built by hand and the namespace is read from the live WSDL rather
than assumed.

Over-fetching 2009+ is deliberate even though training uses 2022 Q1+: the
service could vanish, and fetching twice is the risk, not fetching too much.

Usage:
    python3 scripts/pgw_backfill.py --bucket <raw-bucket>
    python3 scripts/pgw_backfill.py --bucket <raw-bucket> --start 2022 --end 2026
    python3 scripts/pgw_backfill.py --out-dir ./pgw   # local dry run, no S3
"""

from __future__ import annotations

import argparse
import gzip
import re
import sys
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date

PGW_ENDPOINT = "https://opendata.pgworks.com/EUN/EUNService.asmx"
PGW_WSDL = PGW_ENDPOINT + "?wsdl"
OPERATION = "GetEUNHistory"

# December 2009 is where coverage becomes dense (~1,800-3,000 records/month).
DEFAULT_START_YEAR = 2009

USER_AGENT = "station-balance/1.0 (research pipeline)"

SOAP_ENVELOPE = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <{op} xmlns="{ns}">
      <StartDate>{start}</StartDate>
      <EndDate>{end}</EndDate>
    </{op}>
  </soap:Body>
</soap:Envelope>"""


def discover_namespace(wsdl_url: str) -> str:
    """Read targetNamespace off the live WSDL.

    ASMX services default to http://tempuri.org/, but assuming it means a
    silent empty result rather than an error if this one differs -- the SOAP
    body simply would not match any operation.
    """
    try:
        with urllib.request.urlopen(
            urllib.request.Request(wsdl_url, headers={"User-Agent": USER_AGENT}),
            timeout=60,
        ) as resp:
            wsdl = resp.read().decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        print(f"!! could not fetch WSDL ({exc}); falling back to tempuri.org",
              file=sys.stderr)
        return "http://tempuri.org/"

    match = re.search(r'targetNamespace\s*=\s*"([^"]+)"', wsdl)
    if not match:
        print("!! no targetNamespace in WSDL; falling back to tempuri.org",
              file=sys.stderr)
        return "http://tempuri.org/"

    print(f"    namespace: {match.group(1)}")
    return match.group(1)


def quarters(start_year: int, end_year: int):
    """(start, end) date pairs, one per quarter.

    Chunked by quarter because a single whole-history call would be a ~200 MB
    response from a legacy stack, and because a failure then costs one quarter
    rather than everything.
    """
    for year in range(start_year, end_year + 1):
        for q, (m0, m1) in enumerate([(1, 3), (4, 6), (7, 9), (10, 12)], start=1):
            last_day = date(year + (m1 == 12), (m1 % 12) + 1, 1).toordinal() - 1
            yield year, q, date(year, m0, 1), date.fromordinal(last_day)


def call_pgw(namespace: str, start: date, end: date, retries: int = 4) -> bytes:
    # Both dates are YYYYMMDD INTEGERS, not ISO strings -- passing ISO returns
    # an empty result set rather than a fault.
    body = SOAP_ENVELOPE.format(
        op=OPERATION, ns=namespace,
        start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"),
    ).encode("utf-8")

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": f'"{namespace.rstrip("/")}/{OPERATION}"',
        "User-Agent": USER_AGENT,
    }

    last: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(PGW_ENDPOINT, data=body, headers=headers)
            with urllib.request.urlopen(req, timeout=180) as resp:
                return resp.read()
        except (urllib.error.URLError, TimeoutError) as exc:
            last = exc
            backoff = 5 * (attempt + 1)
            print(f"    retry {attempt + 1}/{retries} after {exc} ({backoff}s)",
                  file=sys.stderr)
            time.sleep(backoff)

    raise RuntimeError(f"PGW call failed for {start}..{end}") from last


def count_records(xml_bytes: bytes) -> int:
    """Rough record count, for the sanity check below.

    Counting EUNNUMBER elements rather than parsing the whole payload: the
    response shape is a diffgram whose exact nesting is not worth pinning
    against a service with no versioning. The real parse happens in phase 2.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return -1
    return sum(1 for el in root.iter() if el.tag.rsplit("}", 1)[-1] == "EUNNUMBER")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bucket", help="raw-zone bucket; omit with --out-dir for a dry run")
    ap.add_argument("--prefix", default="raw/closures_pgw")
    ap.add_argument("--out-dir", help="write locally instead of to S3")
    ap.add_argument("--start", type=int, default=DEFAULT_START_YEAR)
    ap.add_argument("--end", type=int, default=date.today().year)
    ap.add_argument("--force", action="store_true",
                    help="re-fetch quarters already present (default: skip)")
    args = ap.parse_args()

    if not args.bucket and not args.out_dir:
        ap.error("one of --bucket or --out-dir is required")

    s3 = None
    if args.bucket:
        import boto3
        s3 = boto3.client("s3")

    out_dir = None
    if args.out_dir:
        import pathlib
        out_dir = pathlib.Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

    print(f"==> PGW backfill {args.start}..{args.end}")
    namespace = discover_namespace(PGW_WSDL)

    total, fetched, skipped = 0, 0, 0
    for year, q, start, end in quarters(args.start, args.end):
        if start > date.today():
            break

        name = f"year={year}/eun_history_{year}q{q}.xml.gz"
        key = f"{args.prefix}/{name}"

        if not args.force and _already_there(s3, args.bucket, key, out_dir, name):
            skipped += 1
            continue

        print(f"    {year} Q{q}  {start} .. {end}")
        payload = call_pgw(namespace, start, end)
        records = count_records(payload)
        total += max(records, 0)

        # Verified dense from December 2009 at ~1,800-3,000 records/month, so a
        # quarter returning nothing after that point is a signal, not a fact.
        if records == 0 and (year, q) > (2009, 4):
            print(f"    !! {year} Q{q} returned 0 records -- unexpected after 2009 Q4",
                  file=sys.stderr)

        blob = gzip.compress(payload)
        if s3:
            s3.put_object(Bucket=args.bucket, Key=key, Body=blob,
                          ContentType="application/gzip")
            print(f"       -> s3://{args.bucket}/{key}  ({records} records, {len(blob)/1024:.0f} KB)")
        else:
            path = out_dir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(blob)
            print(f"       -> {path}  ({records} records, {len(blob)/1024:.0f} KB)")

        fetched += 1
        # The service has no published rate limit and no SLA. One second
        # between calls costs a minute over the whole backfill and keeps this
        # unambiguously a polite client.
        time.sleep(1)

    print(f"\n==> done: {fetched} quarters fetched, {skipped} skipped, ~{total} records")
    print("    This is a ONE-TIME pull. Do not schedule it.")
    return 0


def _already_there(s3, bucket, key, out_dir, name) -> bool:
    if s3:
        from botocore.exceptions import ClientError
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False
    return (out_dir / name).exists()


if __name__ == "__main__":
    sys.exit(main())
