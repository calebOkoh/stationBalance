"""Phase 4 -- Feature Engineering. pipelines.md steps 4.1-4.9.

On the CRS handling (4.1)
-------------------------
pipelines.md recommends reprojecting everything to EPSG:2272 and buffering in
planar coordinates, with GeoPandas or Sedona. Neither is in the EMR Serverless
image, and Sedona was already dropped (.claude/decisions.md).

The risk 4.1 actually exists to prevent is measuring distance in Web Mercator,
where distances inflate ~1.29x at Philadelphia's latitude and the buffer is
silently the wrong size. Computing geodesic distance directly avoids that
failure outright rather than avoiding it via a projection -- at the 150 m scale
the two agree to well under a metre. So distances here are haversine in plain
Spark SQL, and `spark.archives` stays unused.

The same function is used by the inference Lambda, so the buffer test is
identical on both paths -- which is the property 4.4 needs.
"""

from __future__ import annotations

import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from lib import Zones, load_features, parse_args, spark_session

EARTH_RADIUS_M = 6371008.8


def haversine_m(lat1, lon1, lat2, lon2):
    """Geodesic distance as a Spark column expression."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dp, dl = p2 - p1, F.radians(lon2 - lon1)
    a = F.sin(dp / 2) ** 2 + F.cos(p1) * F.cos(p2) * F.sin(dl / 2) ** 2
    return F.lit(2 * EARTH_RADIUS_M) * F.asin(F.sqrt(a))


def closure_features(spark, zones, stations, cfg):
    """4.2 -> 4.4, in that order: filter, JOIN, then expand.

    Join-before-expand is deliberate (.claude/decisions.md). A permit's
    geometry is time-independent, so the buffer test gives an identical result
    either way -- but joining first runs ~108 K point-in-buffer tests instead
    of ~430 K, and only the ~5% that survive get expanded to hours.
    """
    buffer_m = float(cfg["geo"]["station_buffer_m"])

    permits = spark.read.parquet(f"{zones.bronze}/closures_pgw/")

    # 4.2 -- bike relevance. A closure that affects only motor traffic is noise
    # in this model; unfiltered, the closure arm teaches the model nothing.
    bike_net = spark.read.parquet(f"{zones.bronze}/geo/bike_network/")
    permits = permits.join(
        F.broadcast(bike_net.select("segment_id").distinct()),
        permits.segment_id == bike_net.segment_id,
        "left_semi",
    ) if "segment_id" in permits.columns else permits

    # 4.4 -- spatial join, on the permits themselves (not yet expanded).
    near = (
        permits.crossJoin(F.broadcast(stations.select(
            F.col("station_id"), F.col("lat").alias("s_lat"), F.col("lon").alias("s_lon")
        )))
        .withColumn("dist_m", haversine_m(
            F.col("lat"), F.col("lon"), F.col("s_lat"), F.col("s_lon")))
        .filter(F.col("dist_m") <= buffer_m)
    )

    # 4.3 -- NOW expand the survivors to hourly intervals.
    expanded = near.withColumn(
        "hour_ts",
        F.explode(F.sequence(
            F.date_trunc("hour", "construction_start"),
            F.date_trunc("hour", "construction_end"),
            F.expr("INTERVAL 1 HOUR"),
        )),
    )

    return expanded.groupBy("station_id", "hour_ts").agg(
        F.countDistinct("eun_number").alias("n_closures_nearby"),
        F.coalesce(F.sum("closure_length_m"), F.lit(0.0)).alias("closed_bike_lane_m"),
    ).withColumn("has_closure", F.lit(1.0))


def weather_features(spark, zones, stations, cfg):
    """4.5 -- join weather on the LOCAL-time hour, matching 2.2.

    The weather table is ~4 grid points x ~39 K hours, so it broadcasts. Each
    station takes its nearest grid point; at an ~11 km reanalysis grid over a
    city this size, most stations share one.
    """
    weather = spark.read.parquet(f"{zones.bronze}/weather/")

    nearest = (
        stations.crossJoin(F.broadcast(
            weather.select("point_name", "point_lat", "point_lon").distinct()))
        .withColumn("d", haversine_m(F.col("lat"), F.col("lon"),
                                     F.col("point_lat"), F.col("point_lon")))
        .withColumn("rk", F.row_number().over(
            Window.partitionBy("station_id").orderBy("d")))
        .filter(F.col("rk") == 1)
        .select("station_id", "point_name")
    )

    cols = cfg["weather"]["hourly"]
    return (
        F.broadcast(nearest).alias("n")
        .join(weather.alias("w"), F.col("n.point_name") == F.col("w.point_name"))
        .select("n.station_id", "w.hour_ts", *[F.col(f"w.{c}") for c in cols],
                F.col("w.is_daylight"))
    )


def leakage_audit(df, cfg) -> None:
    """4.8 -- every feature is checked against features.yaml, explicitly.

    A feature using data from time > t is unavailable at inference and produces
    a model that looks excellent offline and fails live. This is a gate: an
    undeclared column reaching the modelling table is a hard stop, not a
    warning, because the failure it prevents is silent.
    """
    declared = {name for group in cfg["feature_groups"].values() for name in group}
    keys = {"station_id", "hour_ts", "year", "month"}
    labels = {"net_flow", "is_empty", "is_full", "occupancy", "pct_full",
              "arr", "dep", "reb_in", "reb_out", "capacity", "feasible", "clamped"}

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
    zones = Zones(args.raw_bucket, args.gold_bucket)

    # calendarfeat is the SAME module the inference Lambda imports, shipped via
    # --py-files. That is what makes 4.6 a shared implementation rather than a
    # shared intention.
    import calendarfeat

    labels = spark.read.parquet(f"{zones.silver}/station_hour_labels/")
    stations = spark.read.parquet(f"{zones.bronze}/stations/")

    df = labels.join(F.broadcast(stations.select(
        "station_id", "lat", "lon", "bike_lane_density", "dist_to_centroid_m"
    )), "station_id", "left")

    # 4.5 -- weather
    df = df.join(weather_features(spark, zones, stations, cfg),
                 ["station_id", "hour_ts"], "left")

    # 4.4 -- closures. A station-hour with no nearby closure is a real zero.
    df = df.join(closure_features(spark, zones, stations, cfg),
                 ["station_id", "hour_ts"], "left") \
           .fillna(0, subset=["n_closures_nearby", "closed_bike_lane_m", "has_closure"])

    # 4.6 -- calendar. Registered as a UDF over the shared function so training
    # and serving cannot diverge.
    from pyspark.sql.types import DoubleType, StructField, StructType

    schema = StructType([StructField(n, DoubleType()) for n in calendarfeat.FEATURE_NAMES])
    cal_udf = F.udf(lambda ts: calendarfeat.calendar_features(ts) if ts else None, schema)

    df = df.withColumn("_cal", cal_udf(F.col("hour_ts")))
    for name in calendarfeat.FEATURE_NAMES:
        df = df.withColumn(name, F.col(f"_cal.{name}"))
    df = df.drop("_cal")

    # 4.7 -- lag and rolling. Legitimate: at inference the true current
    # occupancy comes from station_status, so lag-1 IS available at serve time
    # (README section 5). Anything looking forward would not be.
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

    out = df.withColumn("year", F.year("hour_ts")).withColumn("month", F.month("hour_ts"))
    (out.write.mode("overwrite").partitionBy("year", "month")
        .parquet(f"{zones.silver}/station_hour_features/"))

    print(f"[features] wrote {out.count():,} rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
