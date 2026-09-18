"""Phase 4 -- Feature Engineering. pipelines.md steps 4.5-4.8.

Two feature arms, not three. The closure arm is cut: PGW publishes closures as
bare text addresses with no coordinates, and resolving ~108 K of those needs a
street-name normaliser and a centerline range join that do not exist. What
remains is weather and time, plus the lag features.

Everything the CRS handling in 4.1 guarded against is gone with it -- there is
no distance computation left in this pipeline, so there is no projection to get
wrong.
"""

from __future__ import annotations

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from lib import Zones, load_features, parse_args, spark_session

def weather_features(spark, zones, cfg):
    """4.5 -- join weather on the LOCAL-time hour, matching 2.2.

    Every station takes the same grid point. The reanalysis grid is ~11 km and
    Philadelphia's station footprint is smaller than one cell, so per-station
    weather would be the same numbers repeated ~250 times -- features.yaml says
    this explicitly ("2-4 grid points, NOT 250").

    The first grid point in features.yaml wins. Assigning stations to their
    nearest point would need station coordinates, and the only source for those
    was a live feed.
    """
    point = cfg["weather"]["grid_points"][0]["name"]
    cols = cfg["weather"]["hourly"]

    weather = (spark.read.parquet(f"{zones.parsed}/weather/")
               .filter(F.col("point_name") == point)
               .select("hour_ts", *cols, "is_daylight"))

    n = weather.count()
    if n == 0:
        raise SystemExit(f"no weather rows for grid point {point!r} in parsed/weather/")
    print(f"[4.5] weather: {n} hours from grid point {point!r}")

    # Tiny table -- a few tens of thousands of rows -- so it broadcasts.
    return F.broadcast(weather)


def leakage_audit(df, cfg) -> None:
    """4.8 -- every feature is checked against features.yaml, explicitly.

    A feature using data from time > t is unavailable at inference and produces
    a model that looks excellent offline and fails live. This is a gate: an
    undeclared column reaching the modelling table is a hard stop, not a
    warning, because the failure it prevents is silent.
    """
    declared = {name for group in cfg["feature_groups"].values() for name in group}
    keys = {"station_id", "hour_ts", "part_year", "part_month"}
    labels = {"net_flow", "is_empty", "is_full", "occupancy", "pct_full",
              "arr", "dep", "reb_in", "reb_out", "o_initial", "delta", "cum_delta",
              "point_name"}

    present = set(df.columns) - keys - labels
    undeclared = present - declared
    missing = declared - present

    print("\n=== 4.8 leakage audit ===")
    print(f"  declared in features.yaml: {len(declared)}")
    print(f"  present in the table:      {len(present)}")

    if missing:
        print(f"  !! declared but ABSENT: {sorted(missing)}")
    if undeclared:
        raise SystemExit(
            f"leakage audit FAILED -- undeclared columns reached the modelling "
            f"table: {sorted(undeclared)}\n"
            "Every feature must be declared in features.yaml with its time "
            "dependency understood. Drop it or declare it (pipelines.md 4.8)."
        )
    print("  [OK] no undeclared features")


def main() -> int:
    args = parse_args(__doc__)
    spark = spark_session("features")
    cfg = load_features(spark, args.features)
    zones = Zones(args.data_bucket, args.model_bucket)

    # calendarfeat is the SAME module the inference Lambda imports, shipped via
    # --py-files. That is what makes 4.6 a shared implementation rather than a
    # shared intention.
    import calendarfeat

    labels = spark.read.parquet(f"{zones.clean}/station_hour_labels/")

    # 4.5 -- weather, joined on the hour alone (one grid point for the city).
    df = labels.join(weather_features(spark, zones, cfg), ["hour_ts"], "left")

    # 4.6 -- calendar. Registered as a UDF over the shared function so training
    # and serving cannot diverge.
    from pyspark.sql.types import DoubleType, StructField, StructType

    schema = StructType([StructField(n, DoubleType()) for n in calendarfeat.FEATURE_NAMES])
    cal_udf = F.udf(lambda ts: calendarfeat.calendar_features(ts) if ts else None, schema)

    df = df.withColumn("_cal", cal_udf(F.col("hour_ts")))
    for name in calendarfeat.FEATURE_NAMES:
        df = df.withColumn(name, F.col(f"_cal.{name}"))
    df = df.drop("_cal")

    # 4.7 -- lag and rolling. Legitimate: these look only backwards, so any
    # future consumer could compute them from data it already has. Anything
    # looking forward would fail the audit below.
    w = Window.partitionBy("station_id").orderBy(F.col("hour_ts").cast("long"))
    df = (
        df
        .withColumn("occupancy_lag_1h", F.lag("occupancy", 1).over(w))
        .withColumn("net_flow_lag_1h", F.lag("net_flow", 1).over(w))
        .withColumn("net_flow_same_hour_last_week", F.lag("net_flow", 168).over(w))
        .withColumn("net_flow_rolling_7d_mean",
                    F.avg("net_flow").over(w.rangeBetween(-7 * 86400, -3600)))
    )

    leakage_audit(df, cfg)

    out = df.withColumn("part_year", F.year("hour_ts")).withColumn("part_month", F.month("hour_ts"))
    (out.write.mode("overwrite").partitionBy("part_year", "part_month")
        .parquet(f"{zones.clean}/station_hour_features/"))

    print(f"[features] wrote {out.count():,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
