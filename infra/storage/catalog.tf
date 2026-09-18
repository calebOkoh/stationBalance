###############################################################################
# Glue Data Catalog — a read-only convenience, not a dependency.
#
# .claude/decisions.md cut the catalog as a *dependency*: one consumer reads
# Parquet paths directly. What is retained is hand-written DDL so Athena can
# run the ledger diagnostics (pipelines.md 3.10) without anyone writing paths
# by hand. There is deliberately NO crawler — a crawler is a recurring cost
# and a source of schema surprises for tables whose schema is already known.
###############################################################################
locals {
  # Declared here rather than as repeated inline blocks so the schema reads as
  # a list and the two tables stay visually comparable.
  clean_trips_columns = [
    { name = "trip_id", type = "bigint" },
    { name = "duration_s", type = "int" },
    { name = "start_time", type = "timestamp" },
    { name = "end_time", type = "timestamp" },
    { name = "start_station_id", type = "int" },
    { name = "end_station_id", type = "int" },
    { name = "bike_id", type = "string" },
    { name = "bike_type", type = "string" },
    { name = "passholder_type", type = "string" },
  ]

  training_station_hour_columns = [
    { name = "station_id", type = "int" },
    { name = "hour_ts", type = "timestamp" },
    { name = "arr", type = "int" },
    { name = "dep", type = "int" },
    { name = "reb_in", type = "int" },
    { name = "reb_out", type = "int" },
    { name = "net_flow", type = "int" },
    { name = "occupancy", type = "double" },
    { name = "capacity", type = "int" },
    { name = "pct_full", type = "double" },
    { name = "is_empty", type = "boolean" },
    { name = "is_full", type = "boolean" },
  ]
}

resource "aws_glue_catalog_database" "catalog" {
  name        = replace(var.service, "-", "_")
  description = "Indego station capacity lake. Hand-written DDL, no crawler."

  tags = {
    step = "catalog"
  }
}

# The conformed event log (pipelines.md 2.5). Every label-construction step
# reads from here.
resource "aws_glue_catalog_table" "clean_trips" {
  name          = "clean_trips"
  database_name = aws_glue_catalog_database.catalog.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  partition_keys {
    name = "year"
    type = "int"
  }

  partition_keys {
    name = "month"
    type = "int"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.id}/clean/trips/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.clean_trips_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

# The wide modelling table (pipelines.md 5.1) — the one artifact the training
# job reads. Columns beyond the keys and labels are declared by the Spark
# write, so only the stable spine is pinned here.
resource "aws_glue_catalog_table" "training_station_hour" {
  name          = "training_station_hour_features"
  database_name = aws_glue_catalog_database.catalog.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification        = "parquet"
    "parquet.compression" = "SNAPPY"
    EXTERNAL              = "TRUE"
  }

  partition_keys {
    name = "part_year"
    type = "int"
  }

  partition_keys {
    name = "part_month"
    type = "int"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.model.id}/training/station_hour_features/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.training_station_hour_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

###############################################################################
# Athena — runs the QA gates and ledger diagnostics (drawio `ath`).
###############################################################################
resource "aws_athena_workgroup" "qa" {
  name = var.service

  # Destroying the workgroup should not be blocked by query history.
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # A hard ceiling on a single query. The whole lake is 3-5 GB, so any query
    # scanning more than this is a mistake (a missing partition predicate),
    # not a legitimate result. Athena bills per byte scanned, so this is the
    # cost control that matters.
    bytes_scanned_cutoff_per_query = 10 * 1024 * 1024 * 1024

    result_configuration {
      output_location = "s3://${aws_s3_bucket.model.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = {
    step = "quality"
  }
}
