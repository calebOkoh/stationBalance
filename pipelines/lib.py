"""Shared plumbing for the EMR Serverless phase jobs.

Shipped to the executors via --py-files alongside calendarfeat.py, which is the
SAME file the inference Lambda imports -- that is what makes the 4.6 train/serve
contract hold rather than being a comment claiming it does.
"""

from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession


def parse_args(description: str):
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument("--raw-bucket", required=True)
    ap.add_argument("--gold-bucket", required=True)
    ap.add_argument("--glue-database", required=True)
    ap.add_argument("--features", required=True,
                    help="s3:// URI of features.json (rendered from features.yaml by Terraform)")
    return ap.parse_args()


def spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"station-balance/{app_name}")
        # Set explicitly, not left to the cluster default. Every downstream
        # join is on the hour, and a naive local timestamp breaks at DST: one
        # hour duplicates in November and one vanishes in March
        # (pipelines.md 2.2).
        .config("spark.sql.session.timeZone", "America/New_York")
        # Parquet's int96 legacy timestamp path drops sub-second precision and
        # reinterprets the zone. Not wanted anywhere here.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )


def load_features(spark: SparkSession, uri: str) -> dict:
    """Read features.json off S3 through Spark's own filesystem layer.

    Avoids needing boto3 on the driver and works unchanged for a local file
    path during development.
    """
    raw = "".join(spark.sparkContext.textFile(uri).collect())
    return json.loads(raw)


class Zones:
    """The lake paths, in one place.

    Phases address zones by name rather than by literal s3:// strings so a
    mistyped prefix is a NameError at submit time instead of a job that writes
    a thousand objects into the wrong place.
    """

    def __init__(self, raw_bucket: str, gold_bucket: str):
        self.raw = f"s3://{raw_bucket}/raw"
        self.bronze = f"s3://{raw_bucket}/bronze"
        self.silver = f"s3://{raw_bucket}/silver"
        self.gold = f"s3://{gold_bucket}/gold"
        self.models = f"s3://{gold_bucket}/models"


def log_counts(name: str, before: int, after: int) -> None:
    """Report what a filter dropped.

    pipelines.md 2.3 asks for counts dropped per rule specifically: the filters
    are where a silent data-loss bug hides, and a count that looks wrong here
    is far cheaper to notice than a model that mysteriously underperforms.
    """
    dropped = before - after
    pct = (100.0 * dropped / before) if before else 0.0
    print(f"[{name}] {before:,} -> {after:,}  (dropped {dropped:,}, {pct:.2f}%)")
