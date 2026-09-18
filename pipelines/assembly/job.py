"""Phase 5 -- Assembly and Split. pipelines.md steps 5.1-5.5.

Produces the frozen train/val/test splits the training job reads, plus the
baseline table those splits are judged against.
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


def baseline_table(train) -> dict:
    """The (station, hour, weekday) mean net_flow — step 6.1's baseline.

    Exported as an artifact rather than recomputed at training time because it
    is also what any future inference path would need to stand in for the
    week-lag feature, and because a baseline the model is judged against should
    be a frozen number, not something recomputed per experiment.
    """
    rows = (
        train.groupBy("station_id",
                      F.hour("hour_ts").alias("h"),
                      F.dayofweek("hour_ts").alias("dow"))
        .agg(F.avg("net_flow").alias("mean_net_flow"))
        .collect()
    )
    # Spark's dayofweek is 1=Sunday; Python's weekday() is 0=Monday. Converted
    # here so the key format matches calendarfeat's convention.
    return {
        f"{r['station_id']}|{r['h']}|{(r['dow'] + 5) % 7}": round(r["mean_net_flow"], 4)
        for r in rows
    }


def main() -> int:
    args = parse_args(__doc__)
    spark = spark_session("assembly")
    cfg = load_features(spark, args.features)
    zones = Zones(args.data_bucket, args.model_bucket)

    df = spark.read.parquet(f"{zones.clean}/station_hour_features/")

    # 5.1 -- the single wide table the training job reads.
    (df.write.mode("overwrite").partitionBy("part_year", "part_month")
       .parquet(f"{zones.training}/station_hour_features/"))
    spark.sql(f"MSCK REPAIR TABLE `{args.glue_database}`.training_station_hour_features")

    train, val, test = chronological_split(df, cfg)

    # 5.5 -- frozen splits, so model comparisons are like-for-like across
    # experiments. Row counts and date ranges are recorded with them.
    manifest = {}
    for name, part in (("train", train), ("val", val), ("test", test)):
        (part.write.mode("overwrite").parquet(f"{zones.training}/{name}/"))
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

    sc = spark.sparkContext
    sc.parallelize([json.dumps(baseline_table(train))], 1).saveAsTextFile(
        f"{zones.training}/baseline/"
    )
    sc.parallelize([json.dumps(manifest, indent=2)], 1).saveAsTextFile(
        f"{zones.training}/split_manifest/"
    )

    print(f"[assembly] done. {manifest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
