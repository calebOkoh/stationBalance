"""Phase 1 — Land. Converts the downloaded archives into Parquet.

This is pipelines.md step 1.9, and it is the step that makes every later phase
possible: the ingest Lambda writes raw bytes (zips, a CSV, gzipped JSON), and
nothing downstream re-parses raw text. Everything from here on reads Parquet.

Three outputs:

    parsed/trips/      the quarterly archives, unzipped, still unfiltered
    parsed/stations/   station_id + go_live_date
    parsed/weather/    one row per hour, from the Open-Meteo JSON

No cleaning happens here. Filtering, schema normalisation and deduplication are
phase 2's job, and keeping them separate means a bad filter rule can be fixed
and re-run without re-downloading or re-unzipping anything.
"""

from __future__ import annotations

import gzip
import io
import json
import sys
import zipfile

from pyspark.sql import functions as F
from pyspark.sql import types as T

from lib import Zones, load_features, parse_args, spark_session


def land_trips(spark, zones, bucket) -> int:
    """Unzip the trip archives.

    Done on the driver with the stdlib: 18 archives at ~6 MB each is a
    sequential read, and Spark cannot read inside a zip without a custom input
    format written for the occasion.
    """
    import boto3

    s3 = boto3.client("s3")
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/trips/")
    keys = [o["Key"] for page in pages for o in page.get("Contents", [])
            if o["Key"].endswith(".zip")]

    if not keys:
        raise SystemExit("no trip archives under raw/trips/ — run scripts/10_ingest.sh")

    staged = 0
    for key in sorted(keys):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            for member in zf.namelist():
                if not member.lower().endswith(".csv"):
                    continue
                out_key = (key.replace("raw/trips/", "parsed/_trips_csv/")
                              .replace(".zip", f"/{member.rsplit('/', 1)[-1]}"))
                s3.put_object(Bucket=bucket, Key=out_key, Body=zf.read(member))
                staged += 1
                print(f"  {key} -> {out_key}")

    # Read back as text-typed columns. Casting is phase 2's job -- doing it here
    # would silently null a value whose format changed between quarters, and
    # that is exactly the drift phase 2 exists to catch explicitly.
    #
    # recursiveFileLookup rather than a glob: the staged keys nest
    # year=/quarter=/<archive>/<file>.csv, and a fixed-depth glob silently
    # matches nothing the moment that shape changes. It also switches off
    # partition inference, which is wanted -- year/month are re-derived from
    # the parsed timestamps later, not trusted from a path.
    df = (spark.read
          .option("header", True)
          .option("recursiveFileLookup", "true")
          .csv(f"{zones.parsed}/_trips_csv/"))

    rows = df.count()
    if rows == 0:
        raise SystemExit(
            f"staged {staged} CSV members but read 0 rows from "
            f"{zones.parsed}/_trips_csv/ -- the archives are present but "
            "unreadable, which is a different problem from them being absent"
        )

    df.write.mode("overwrite").parquet(f"{zones.parsed}/trips/")
    print(f"[land] trips: {staged} CSV members, {rows:,} rows -> parsed/trips/")
    return staged


def land_stations(spark, zones) -> int:
    """The station table.

    Only two columns are actually needed: the id, and the go-live date that
    step 3.4 uses so a station is not scored as zero-demand for the years
    before it existed. Column names in this CSV have drifted over the years,
    so they are matched case-insensitively rather than assumed.
    """
    raw = spark.read.option("header", True).csv(f"{zones.raw}/stations/*.csv")
    lowered = {c.lower().replace(" ", "_"): c for c in raw.columns}

    def pick(*candidates):
        for c in candidates:
            if c in lowered:
                return F.col(f"`{lowered[c]}`")
        raise SystemExit(
            f"station CSV has none of {candidates}; columns present: {raw.columns}"
        )

    df = raw.select(
        pick("station_id", "station", "id").cast("int").alias("station_id"),
        # "Day of Go_live_date" is what the 2026-07-15 vintage publishes -- a
        # Tableau export header, not a rename of the underlying field.
        pick("go_live_date", "golivedate", "go_live",
             "day_of_go_live_date").alias("go_live_raw"),
        pick("station_name", "name").alias("station_name"),
    ).withColumn(
        # Published as M/D/YYYY in some vintages and YYYY-MM-DD in others.
        "go_live_date",
        F.coalesce(
            F.to_timestamp("go_live_raw", "M/d/yyyy"),
            F.to_timestamp("go_live_raw", "yyyy-MM-dd"),
        ),
    ).drop("go_live_raw").filter(F.col("station_id").isNotNull())

    n = df.count()
    if n == 0:
        raise SystemExit("station table parsed to zero rows")

    undated = df.filter(F.col("go_live_date").isNull()).count()
    if undated:
        # Not fatal, but it means those stations get scored from the start of
        # the window, which understates their demand. Worth seeing.
        print(f"[land] !! {undated}/{n} stations have an unparseable go_live_date")

    df.write.mode("overwrite").parquet(f"{zones.parsed}/stations/")
    print(f"[land] stations: {n} rows -> parsed/stations/")
    return n


def land_weather(spark, zones, bucket, cfg) -> int:
    """Flatten the Open-Meteo responses into one row per hour.

    The API returns parallel arrays -- `hourly.time` alongside `hourly.<var>` --
    so this zips them into records. Done on the driver because the whole pull
    is ~40 small JSON documents covering ~39 K hours.

    `is_daylight` is derived here from the daily sunrise/sunset pair rather
    than from an hour-of-day rule, so it tracks the actual season.
    """
    import boto3
    from datetime import datetime

    s3 = boto3.client("s3")
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="raw/weather/")
    keys = [o["Key"] for page in pages for o in page.get("Contents", [])
            if o["Key"].endswith(".json.gz")]

    if not keys:
        raise SystemExit("no weather under raw/weather/ — run scripts/10_ingest.sh weather")

    variables = cfg["weather"]["hourly"]
    records: list[dict] = []

    for key in sorted(keys):
        blob = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        payload = json.loads(gzip.decompress(blob))
        point = payload.get("_point", {}).get("name", "unknown")

        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])

        daily = payload.get("daily", {})
        sun = {}
        for d, rise, sett in zip(daily.get("time", []),
                                 daily.get("sunrise", []),
                                 daily.get("sunset", [])):
            if rise and sett:
                sun[d] = (rise, sett)

        for i, ts in enumerate(times):
            row = {"point_name": point, "hour_ts": ts}
            for v in variables:
                series = hourly.get(v) or []
                value = series[i] if i < len(series) else None
                # Open-Meteo returns whole numbers unquoted, so a 100% humidity
                # or a 0 mm precipitation arrives as a Python int. DoubleType
                # rejects int outright rather than widening it, and which
                # variables come back integral varies by hour -- so coerce here
                # rather than per-variable.
                row[v] = None if value is None else float(value)

            day = ts[:10]
            if day in sun:
                rise, sett = sun[day]
                row["is_daylight"] = 1.0 if rise <= ts <= sett else 0.0
            else:
                row["is_daylight"] = None
            records.append(row)

    schema = T.StructType(
        [T.StructField("point_name", T.StringType()),
         T.StructField("hour_ts", T.StringType())]
        + [T.StructField(v, T.DoubleType()) for v in variables]
        + [T.StructField("is_daylight", T.DoubleType())]
    )

    df = (spark.createDataFrame(records, schema)
          .withColumn("hour_ts", F.to_timestamp("hour_ts"))
          .dropDuplicates(["point_name", "hour_ts"]))

    n = df.count()
    df.write.mode("overwrite").parquet(f"{zones.parsed}/weather/")
    print(f"[land] weather: {n} point-hours from {len(keys)} files -> parsed/weather/")
    return n


def main() -> int:
    args = parse_args(__doc__)
    spark = spark_session("land")
    cfg = load_features(spark, args.features)
    zones = Zones(args.data_bucket, args.model_bucket)

    land_trips(spark, zones, args.data_bucket)
    land_stations(spark, zones)
    land_weather(spark, zones, args.data_bucket, cfg)

    print("[land] done — parsed/ is ready for phase 2")
    return 0


if __name__ == "__main__":
    sys.exit(main())
