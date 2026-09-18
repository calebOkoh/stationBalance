"""Phase 2 -- Conform (Bronze -> Silver). pipelines.md steps 1.9 and 2.1-2.5.

Reads the raw trip archives, normalises 18 quarters of drifting schema onto one
shape, localises timestamps, filters, deduplicates, and writes silver.trips.

Step 2.6 (resolving PGW addresses to geometry) is the project's highest-risk
item and is NOT implemented here -- it needs a street-name normaliser and a
centerline range join that do not exist yet. The closure arm degrades to permit
counts per tract-hour if it does not clear its timebox (.claude/decisions.md).
That gap is explicit rather than silently producing an empty closure table.
"""

from __future__ import annotations

import sys
import zipfile

from pyspark.sql import functions as F
from pyspark.sql import types as T

from lib import Zones, load_features, log_counts, parse_args, spark_session

# Column-name drift across the quarterly archives. An EXPLICIT mapping,
# asserted rather than inferred: a union over inferred schemas fails outright
# or -- far worse -- silently nulls a column that changed case
# (pipelines.md 2.1).
COLUMN_ALIASES = {
    "trip_id": ["trip_id", "tripid", "Trip ID"],
    "duration": ["duration", "Duration"],
    "start_time": ["start_time", "starttime", "Start Time"],
    "end_time": ["end_time", "endtime", "End Time"],
    "start_station": ["start_station", "startstation", "Start Station"],
    "end_station": ["end_station", "endstation", "End Station"],
    "bike_id": ["bike_id", "bikeid", "Bike ID"],
    "bike_type": ["bike_type", "biketype", "Bike Type"],
    "passholder_type": ["passholder_type", "passholdertype", "Passholder Type"],
}

# The archives are zipped CSVs. Spark cannot read inside a zip, so the raw
# objects are expanded once into /bronze rather than being re-expanded by every
# later phase.
TRIP_SCHEMA = T.StructType([
    T.StructField("trip_id", T.StringType()),
    T.StructField("duration", T.StringType()),
    T.StructField("start_time", T.StringType()),
    T.StructField("end_time", T.StringType()),
    T.StructField("start_station", T.StringType()),
    T.StructField("end_station", T.StringType()),
    T.StructField("bike_id", T.StringType()),
    T.StructField("bike_type", T.StringType()),
    T.StructField("passholder_type", T.StringType()),
])


def resolve_columns(df):
    """Map whatever this quarter called a column onto the canonical name."""
    lowered = {c.lower().replace(" ", "_"): c for c in df.columns}
    selected, missing = [], []

    for canonical, aliases in COLUMN_ALIASES.items():
        source = next(
            (lowered[a.lower().replace(" ", "_")] for a in aliases
             if a.lower().replace(" ", "_") in lowered),
            None,
        )
        if source is None:
            # bike_type is genuinely absent pre-2018 rather than renamed, so it
            # is nulled. Anything else missing is a schema change nobody has
            # looked at, and guessing would corrupt the ledger downstream.
            if canonical in ("bike_type", "passholder_type"):
                selected.append(F.lit(None).cast("string").alias(canonical))
                continue
            missing.append(canonical)
        else:
            selected.append(F.col(f"`{source}`").alias(canonical))

    if missing:
        raise SystemExit(
            f"schema map does not cover {missing}; columns present: {df.columns}\n"
            "Add the alias to COLUMN_ALIASES or drop that quarter (pipelines.md 2.1)."
        )

    return df.select(*selected)


def expand_archives(spark, zones) -> None:
    """1.9 -- land the zipped CSVs as Bronze Parquet.

    Done on the driver with the stdlib zipfile module: 18 archives at ~6 MB
    each is a sequential read, and distributing it would mean a custom input
    format for no gain.
    """
    import boto3

    s3 = boto3.client("s3")
    bucket = zones.raw.split("/")[2]
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="raw/trips/"
    )

    keys = [o["Key"] for page in pages for o in page.get("Contents", [])
            if o["Key"].endswith(".zip")]
    if not keys:
        raise SystemExit("no trip archives in /raw/trips/ -- run scripts/10_ingest.sh")

    print(f"[conform] expanding {len(keys)} archives")
    for key in sorted(keys):
        import io
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            for member in zf.namelist():
                if not member.lower().endswith(".csv"):
                    continue
                out_key = key.replace("raw/trips/", "bronze/trips/").replace(
                    ".zip", f"/{member.rsplit('/', 1)[-1]}"
                )
                s3.put_object(Bucket=bucket, Key=out_key, Body=zf.read(member))
                print(f"  {key} -> {out_key}")


def main() -> int:
    args = parse_args(__doc__)
    spark = spark_session("conform")
    cfg = load_features(spark, args.features)
    zones = Zones(args.raw_bucket, args.gold_bucket)

    expand_archives(spark, zones)

    raw = spark.read.option("header", True).csv(f"{zones.bronze}/trips/*/*/*.csv")
    df = resolve_columns(raw)
    before = df.count()

    # 2.2 -- parse and localise. The session timezone is already America/New_York
    # (lib.spark_session), so to_timestamp lands in local time directly.
    df = (
        df
        .withColumn("start_time", F.to_timestamp("start_time"))
        .withColumn("end_time", F.to_timestamp("end_time"))
        .withColumn("duration_s",
                    F.when(F.col("duration").rlike(r"^\d+$"),
                           F.col("duration").cast("int") * 60)
                     .otherwise(F.unix_timestamp("end_time") - F.unix_timestamp("start_time")))
        .withColumn("start_station_id", F.col("start_station").cast("int"))
        .withColumn("end_station_id", F.col("end_station").cast("int"))
        .withColumn("trip_id", F.col("trip_id").cast("long"))
    )

    # 2.3 -- filter. Virtual Station rows are staff check-in artifacts and
    # corrupt the Phase 3 mass balance; sub-minute trips are dock-repick noise,
    # not demand.
    min_s = cfg["labels"]["min_trip_seconds"]
    max_s = cfg["labels"]["max_trip_hours"] * 3600
    window_start = cfg["project"]["train_window_start"]

    df = df.filter(
        F.col("start_station_id").isNotNull()
        & F.col("end_station_id").isNotNull()
        & (F.lower(F.coalesce(F.col("start_station"), F.lit(""))) != "virtual station")
        & (F.lower(F.coalesce(F.col("end_station"), F.lit(""))) != "virtual station")
        & (F.col("duration_s") > min_s)
        & (F.col("duration_s") <= max_s)
        & (F.col("start_time") >= F.lit(window_start).cast("timestamp"))
    )
    after_filter = df.count()
    log_counts("2.3 filter", before, after_filter)

    # 2.4 -- deduplicate. Quarterly files overlap at boundaries, and a
    # duplicated trip double-counts an arrival, which breaks the ledger.
    df = df.dropDuplicates(["trip_id"])
    log_counts("2.4 dedupe", after_filter, df.count())

    out = (
        df.select(
            "trip_id", "duration_s", "start_time", "end_time",
            "start_station_id", "end_station_id",
            "bike_id", "bike_type", "passholder_type",
        )
        .withColumn("year", F.year("start_time"))
        .withColumn("month", F.month("start_time"))
    )

    (out.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{zones.silver}/trips/"))

    # Register partitions so Athena can run the QA gates without anyone
    # writing paths by hand. The table DDL itself is Terraform-managed.
    spark.sql(f"MSCK REPAIR TABLE `{args.glue_database}`.silver_trips")

    print(f"[conform] wrote silver.trips")
    return 0


if __name__ == "__main__":
    sys.exit(main())
