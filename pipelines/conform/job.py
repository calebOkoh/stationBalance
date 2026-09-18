"""Phase 2 -- Conform. pipelines.md steps 2.1-2.5.

Reads parsed/trips/, normalises 18 quarters of drifting schema onto
one shape, localises timestamps, filters, deduplicates, and writes clean/trips/.

Also conforms the station table into clean/stations/. Filtering the station
list belongs here for the same reason filtering trips does: phase 1 lands what
was published without judgement, and a bad filter rule is then fixable without
re-downloading anything.

Step 2.6 (resolving PGW closure addresses to geometry) is gone along with the
rest of the closure arm: PGW publishes addresses as bare text with no
coordinates, and the street-name normaliser that would fix that does not exist.
The project measures weather and time, not closures.
"""

from __future__ import annotations

import sys

from pyspark.sql import functions as F

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



def _parse_ts(column: str):
    """Parse a published trip timestamp, trying the known formats in order."""
    return F.coalesce(
        F.to_timestamp(column, "M/d/yyyy H:mm"),
        F.to_timestamp(column, "M/d/yyyy H:mm:ss"),
        F.to_timestamp(column, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(column, "yyyy-MM-dd'T'HH:mm:ss"),
    )


def assert_timestamps_parsed(df, before: int) -> None:
    """Stop if the timestamp format has drifted out from under the parser.

    An unparseable timestamp is NULL, and a NULL is dropped by the 2.3 window
    filter without comment -- so the failure mode is an empty clean/trips/ and
    a green job, which is the single most expensive kind of bug in this
    pipeline. Assert it instead (pipelines.md 2.1's rule, applied to values
    rather than to column names).
    """
    bad = df.filter(F.col("start_time").isNull() | F.col("end_time").isNull()).count()
    pct = (100.0 * bad / before) if before else 0.0
    print(f"[2.2 parse] {bad:,}/{before:,} rows ({pct:.2f}%) have an unparseable timestamp")

    if pct > 1.0:
        raise SystemExit(
            f"2.2 timestamp parsing FAILED: {pct:.2f}% of rows are unparseable.\n"
            "The published format has changed. Add it to _parse_ts() rather "
            "than letting the 2.3 filter drop the rows silently."
        )


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


def conform_stations(spark, zones) -> list[int]:
    """The station table -- drop the pseudo-stations before 3.4 sees them.

    `Virtual Station` is Indego's staff check-in/out artifact, not a physical
    dock. Step 2.3 already drops it from TRIPS. Without the same rule applied
    to the station TABLE it survives into the step 3.4 grid and is scored as a
    real station for every hour of the window: ~41 K rows carrying
    `is_empty = True` by construction, on the rare positive class, plus a
    contribution to the 3.10 stockout diagnostic that has nothing to do with
    ledger drift (.claude/decisions.md).

    Matched by NAME, case-insensitively, because the station table is the ONLY
    published file that carries station names -- the trip archives identify a
    station by bare numeric id. So this function is also where the exclusion
    list comes from: it returns the ids it dropped, and the trip filter in
    main() uses them. One source of truth, and no hardcoded 3000.
    """
    stations = spark.read.parquet(f"{zones.parsed}/stations/")
    before = stations.count()

    virtual = stations.filter(
        F.lower(F.coalesce(F.col("station_name"), F.lit(""))) == "virtual station"
    )
    excluded = sorted(r["station_id"] for r in virtual.select("station_id").collect())

    out = stations.filter(~F.col("station_id").isin(excluded)) if excluded else stations
    log_counts("2.3 stations filter", before, out.count())
    print(f"[conform] pseudo-station ids excluded: {excluded}")

    out.write.mode("overwrite").parquet(f"{zones.clean}/stations/")
    print("[conform] wrote clean/stations/")
    return excluded


def main() -> int:
    args = parse_args(__doc__)
    spark = spark_session("conform")
    cfg = load_features(spark, args.features)
    zones = Zones(args.data_bucket, args.model_bucket)

    # The station table first: it is the only published file carrying station
    # NAMES, so it is where the pseudo-station id list comes from.
    excluded_stations = conform_stations(spark, zones)

    df = resolve_columns(spark.read.parquet(f"{zones.parsed}/trips/"))
    before = df.count()

    # 2.2 -- parse and localise. The session timezone is already America/New_York
    # (lib.spark_session), so to_timestamp lands in local time directly.
    #
    # The format is EXPLICIT. Every archive publishes `M/d/yyyy H:mm`
    # ("1/1/2022 0:04"), and bare to_timestamp() assumes `yyyy-MM-dd HH:mm:ss`
    # and returns NULL for all 5.3 M rows -- which the 2.3 window filter then
    # drops silently, leaving an empty clean/trips/ and a job that reports
    # success. Coalesced over the variants rather than pinned to one, because
    # the published format has drifted before and the assert below is what
    # catches it if it drifts again.
    df = (
        df
        .withColumn("start_time", _parse_ts("start_time"))
        .withColumn("end_time", _parse_ts("end_time"))
        .withColumn("duration_s",
                    F.when(F.col("duration").rlike(r"^\d+$"),
                           F.col("duration").cast("int") * 60)
                     .otherwise(F.unix_timestamp("end_time") - F.unix_timestamp("start_time")))
        .withColumn("start_station_id", F.col("start_station").cast("int"))
        .withColumn("end_station_id", F.col("end_station").cast("int"))
        .withColumn("trip_id", F.col("trip_id").cast("long"))
    )

    assert_timestamps_parsed(df, before)

    # 2.3 -- filter. Virtual Station rows are staff check-in artifacts and
    # corrupt the Phase 3 mass balance; sub-minute trips are dock-repick noise,
    # not demand.
    min_s = cfg["labels"]["min_trip_seconds"]
    max_s = cfg["labels"]["max_trip_hours"] * 3600
    window_start = cfg["project"]["train_window_start"]

    df = df.filter(
        F.col("start_station_id").isNotNull()
        & F.col("end_station_id").isNotNull()
        # By ID. The trip archives identify a station by bare numeric id and
        # carry no name column at all, so the name comparison this replaces
        # never matched anything and every Virtual Station trip survived.
        # The ids come from conform_stations() above, which has the names.
        & (~F.col("start_station_id").isin(excluded_stations))
        & (~F.col("end_station_id").isin(excluded_stations))
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

    # duration_s comes out as bigint from the unix_timestamp branch; the Glue
    # DDL publishes int. Same reasoning as the labels job: the catalog is the
    # contract and the job conforms to it.
    out = (
        df.select(
            "trip_id",
            F.col("duration_s").cast("int").alias("duration_s"),
            "start_time", "end_time",
            "start_station_id", "end_station_id",
            "bike_id", "bike_type", "passholder_type",
        )
        .withColumn("year", F.year("start_time"))
        .withColumn("month", F.month("start_time"))
    )

    (out.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{zones.clean}/trips/"))

    # Register partitions so Athena can run the QA gates without anyone
    # writing paths by hand. The table DDL itself is Terraform-managed.
    spark.sql(f"MSCK REPAIR TABLE `{args.glue_database}`.clean_trips")

    print("[conform] wrote clean/trips/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
