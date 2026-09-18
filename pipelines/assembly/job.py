"""Phase 5 -- Assembly and Split. pipelines.md steps 5.1-5.5.

Produces the frozen train/val/test splits the training job reads, and the
serving context the inference Lambda needs. Both come out of the SAME pass, so
a feature computed for training and its serve-time counterpart are produced by
one piece of code rather than two that agree today.
"""

from __future__ import annotations

import json
import sys

from pyspark.sql import functions as F

from lib import Zones, load_features, parse_args, spark_session


def chronological_split(df, cfg):
    """5.2 -- TEMPORAL split. Never random.

    A random split leaks future weather and future station behaviour into
    training and badly inflates every metric. A chronological cut is the only
    honest estimate of live performance, and it is the difference between a
    reported number that survives contact with reality and one that does not.
    """
    split = cfg["split"]
    train_end = F.lit(split["train_end"]).cast("timestamp")
    val_end = F.lit(split["val_end"]).cast("timestamp")
    test_end = F.lit(split["test_end"]).cast("timestamp")

    train = df.filter(F.col("hour_ts") <= train_end)
    val = df.filter((F.col("hour_ts") > train_end) & (F.col("hour_ts") <= val_end))
    test = df.filter((F.col("hour_ts") > val_end) & (F.col("hour_ts") <= test_end))

    return train, val, test


def class_weight(train, cfg) -> float:
    """5.4 -- is_empty is rare.

    Untreated, a classifier scores high accuracy by never predicting "empty",
    which is exactly the prediction the web tool exists to make. Class weights
    rather than resampling: resampling a time series breaks the temporal
    structure the lag features depend on.
    """
    counts = train.groupBy("is_empty").count().collect()
    by_class = {bool(r["is_empty"]): r["count"] for r in counts}
    pos, neg = by_class.get(True, 0), by_class.get(False, 0)

    if pos == 0:
        print("[5.4] !! no positive is_empty rows in train -- classifier cannot be fit")
        return 1.0

    weight = neg / pos
    print(f"[5.4] is_empty positive rate {100.0 * pos / (pos + neg):.2f}%  "
          f"scale_pos_weight={weight:.1f}")
    return weight


def serving_context(train, stations, cfg) -> dict:
    """The serve-time substitutes for features that need history.

    Exported HERE, from the training data, by the same code that built the
    training rows. The alternative -- the inference Lambda deriving them
    independently -- is the classic train/serve skew this whole phase exists to
    prevent.

    `climatology` is the (station, hour, weekday) mean net_flow, which is also
    the 6.1 baseline table. It stands in at serve time for
    net_flow_same_hour_last_week, whose true value needs a week of reconstructed
    ledger that only exists after a Phase 3 run.
    """
    clim = (
        train.groupBy("station_id",
                      F.hour("hour_ts").alias("h"),
                      F.dayofweek("hour_ts").alias("dow"))
        .agg(F.avg("net_flow").alias("mean_net_flow"))
        .collect()
    )
    # Spark's dayofweek is 1=Sunday; Python's weekday() is 0=Monday. Converting
    # here rather than at read time keeps the key format identical on both
    # sides of the contract.
    climatology = {
        f"{r['station_id']}|{r['h']}|{(r['dow'] + 5) % 7}": round(r["mean_net_flow"], 4)
        for r in clim
    }

    rolling = {
        str(r["station_id"]): round(r["m"], 4)
        for r in train.groupBy("station_id").agg(F.avg("net_flow").alias("m")).collect()
    }

    static = {
        str(r["station_id"]): {
            "capacity": r["capacity_gbfs"],
            "lat": r["lat"],
            "lon": r["lon"],
            "bike_lane_density": r["bike_lane_density"],
            "dist_to_centroid_m": r["dist_to_centroid_m"],
        }
        for r in stations.collect()
    }

    return {
        "station_static": static,
        "climatology": climatology,
        "net_flow_rolling_7d_mean": rolling,
    }


def main() -> int:
    args = parse_args(__doc__)
    spark = spark_session("assembly")
    cfg = load_features(spark, args.features)
    zones = Zones(args.raw_bucket, args.gold_bucket)

    df = spark.read.parquet(f"{zones.silver}/station_hour_features/")
    stations = spark.read.parquet(f"{zones.bronze}/stations/")

    # 5.1 -- the single wide table the training job reads.
    (df.write.mode("overwrite").partitionBy("year", "month")
       .parquet(f"{zones.gold}/station_hour_features/"))
    spark.sql(f"MSCK REPAIR TABLE `{args.glue_database}`.gold_station_hour_features")

    train, val, test = chronological_split(df, cfg)

    # 5.5 -- frozen splits, so model comparisons are like-for-like across
    # experiments. Row counts and date ranges are recorded with them.
    manifest = {}
    for name, part in (("train", train), ("val", val), ("test", test)):
        (part.write.mode("overwrite").parquet(f"{zones.gold}/{name}/"))
        bounds = part.agg(F.min("hour_ts"), F.max("hour_ts")).first()
        manifest[name] = {
            "rows": part.count(),
            "from": str(bounds[0]),
            "to": str(bounds[1]),
        }
        print(f"[5.5] {name}: {manifest[name]}")

    # 5.3 is deliberately NOT done here. LightGBM needs no scaling and the
    # categoricals are already ordinal-encoded upstream, so there is no
    # transformer to fit -- and fitting one on the full dataset "because the
    # step says so" would leak test statistics for no benefit.
    manifest["class_weight_is_empty"] = class_weight(train, cfg)

    context = serving_context(train, stations, cfg)
    sc = spark.sparkContext
    sc.parallelize([json.dumps(context)], 1).saveAsTextFile(
        f"{zones.gold}/serving_context/"
    )
    sc.parallelize([json.dumps(manifest, indent=2)], 1).saveAsTextFile(
        f"{zones.gold}/split_manifest/"
    )

    print(f"[assembly] done. {manifest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
