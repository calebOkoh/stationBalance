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
    ap.add_argument("--data-bucket", required=True)
    ap.add_argument("--model-bucket", required=True)
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
    """The storage paths, in one place.

    Phases address these by name rather than by literal s3:// strings, so a
    mistyped prefix is a NameError at submit time instead of a job that writes
    a thousand objects somewhere nobody looks.

        raw       exactly what was downloaded, byte for byte. Never written
                  twice, never modified. Everything is reproducible from here.
        parsed    the same data converted to Parquet. No cleaning, no filtering.
        clean     conformed, filtered, deduplicated, and labelled.
        training  the wide modelling table and the frozen train/val/test splits.
        models    exported model artifacts.
    """

    def __init__(self, data_bucket: str, model_bucket: str):
        self.raw = f"s3://{data_bucket}/raw"
        self.parsed = f"s3://{data_bucket}/parsed"
        self.clean = f"s3://{data_bucket}/clean"
        self.training = f"s3://{model_bucket}/training"
        self.models = f"s3://{model_bucket}/models"


def log_counts(name: str, before: int, after: int) -> None:
    """Report what a filter dropped.

    pipelines.md 2.3 asks for counts dropped per rule specifically: the filters
    are where a silent data-loss bug hides, and a count that looks wrong here
    is far cheaper to notice than a model that mysteriously underperforms.
    """
    dropped = before - after
    pct = (100.0 * dropped / before) if before else 0.0
    print(f"[{name}] {before:,} -> {after:,}  (dropped {dropped:,}, {pct:.2f}%)")
