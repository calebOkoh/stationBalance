###############################################################################
# Pipeline layer — the batch compute the architecture draws: EMR Serverless for
# phases 2-5, SageMaker for phase 6, and the model package group that is the
# handoff to any future inference path.
#
# Nothing in this layer runs on a schedule. Pipeline 1 "runs a handful of
# times, attended" (.claude/decisions.md), so ordering comes from the numbered
# scripts in scripts/ rather than from Step Functions. What Terraform owns here
# is the capacity to run a phase, not the decision to run one.
###############################################################################

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

data "aws_s3_bucket" "data" {
  bucket = coalesce(
    var.data_bucket_name,
    "${var.service}-data-${data.aws_caller_identity.current.account_id}",
  )
}

data "aws_s3_bucket" "model" {
  bucket = coalesce(
    var.model_bucket_name,
    "${var.service}-model-${data.aws_caller_identity.current.account_id}",
  )
}

###############################################################################
# EMR Serverless — drawio `emr`, phases 2-5
#
# preInitializedCapacity is absent, which means zero. That is load-bearing:
# pre-initialised capacity is a standing hourly charge for an application that
# runs a handful of times (.claude/decisions.md), and it is the single easiest
# way to turn a $5/month project into a $200/month one.
#
# The image is stock. Apache Sedona was dropped -- it is absent from the
# default image, and ~430 K closure-hours against ~250 stations is a GeoPandas
# job that fits in memory, so a custom image would buy nothing.
###############################################################################
resource "aws_emrserverless_application" "spark" {
  name          = var.service
  release_label = var.emr_release_label
  type          = "SPARK"

  # The application idles to zero after 15 minutes. Combined with no
  # pre-initialised capacity, an application nobody is using costs nothing.
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  auto_start_configuration {
    enabled = true
  }

  # EMR Serverless populates this itself on creation. Declaring it explicitly
  # so Terraform does not try to REMOVE it on the next apply -- an application
  # in STARTED state rejects that, and the error names maximumCapacity as
  # updatable, which sends you looking at the wrong attribute.
  #
  # One concurrent run is the honest number: the phases are sequential and run
  # by hand, so anything higher only lets a mistyped second submit compete for
  # the same vCPU quota.
  scheduler_configuration {
    max_concurrent_runs   = 1
    queue_timeout_minutes = 360
  }

  # A hard ceiling, not a target. Phases 2-5 operate on a ~10 M-row grid that
  # fits in memory; this exists so a runaway job cannot scale into real money.
  #
  # Capped at the account's concurrent-vCPU quota. A maximumCapacity above the
  # quota is accepted by the API and then fails at executor allocation, so the
  # min() is what turns a mid-job crash into a config that simply fits.
  maximum_capacity {
    cpu    = "${min((var.executor_count + 1) * 4, var.max_concurrent_vcpu)} vCPU"
    memory = "${min((var.executor_count + 1) * 16, var.max_concurrent_vcpu * 4)} GB"
  }

  lifecycle {
    precondition {
      condition     = (var.executor_count + 1) * 4 <= var.max_concurrent_vcpu
      error_message = "One driver plus ${var.executor_count} executors at 4 vCPU each needs ${(var.executor_count + 1) * 4} vCPU, above the account quota of ${var.max_concurrent_vcpu}. Lower executor_count, or raise quota L-D05C8A75 and set max_concurrent_vcpu to match."
    }
  }

  tags = {
    step = "processing"
  }
}

###############################################################################
# IAM — EMR Serverless job role
###############################################################################
data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "emr" {
  name               = "${var.service}-emr-job"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json

  tags = {
    step = "processing"
  }
}

data "aws_iam_policy_document" "emr" {
  # Reads raw/, writes parsed/ and clean/. It does NOT get write access to
  # raw/ -- those are the downloaded archives, and a Spark job with a bad
  # output path is exactly how that invariant gets broken.
  statement {
    sid       = "ReadRawArchives"
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_s3_bucket.data.arn}/raw/*"]
  }

  statement {
    sid = "WriteDerivedData"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${data.aws_s3_bucket.data.arn}/parsed/*",
      "${data.aws_s3_bucket.data.arn}/clean/*",
      "${data.aws_s3_bucket.model.arn}/training/*",
      "${data.aws_s3_bucket.model.arn}/models/*",
      "${data.aws_s3_bucket.model.arn}/emr-logs/*",

      # EMRFS writes a zero-byte marker per parent level when it creates a
      # "directory". For parsed/trips/ that is parsed/trips_$folder$ (covered by
      # the prefixes above) AND parsed_$folder$ at the bucket ROOT, which has no
      # slash after the prefix and so matches none of them.
      "${data.aws_s3_bucket.data.arn}/parsed_$folder$",
      "${data.aws_s3_bucket.data.arn}/clean_$folder$",
      "${data.aws_s3_bucket.model.arn}/training_$folder$",
      "${data.aws_s3_bucket.model.arn}/models_$folder$",
      "${data.aws_s3_bucket.model.arn}/emr-logs_$folder$",
    ]
  }

  statement {
    sid       = "ReadJobCode"
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_s3_bucket.model.arn}/code/*"]
  }

  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [data.aws_s3_bucket.data.arn, data.aws_s3_bucket.model.arn]
  }

  # Catalog access is read-plus-partition-registration only. The catalog is a
  # convenience over Parquet paths, not a system of record, so nothing here
  # may drop a table.
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases",
      "glue:GetTable", "glue:GetTables",
      "glue:GetPartition", "glue:GetPartitions",
      "glue:BatchCreatePartition", "glue:CreatePartition",
      "glue:UpdatePartition", "glue:BatchGetPartition",
      # MSCK REPAIR TABLE rewrites the table's partition metadata, which Glue
      # authorises as UpdateTable rather than as a partition action. Still no
      # Create or Delete: the DDL stays Terraform-owned, and a job can register
      # partitions on an existing table but cannot add or drop one.
      "glue:UpdateTable",
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${replace(var.service, "-", "_")}",
      # AWSGlueDataCatalogHiveClientFactory verifies that `default` exists on
      # EVERY spark.sql() call, before it looks at the database the query names.
      # Without GetDatabase on it, every phase that touches SQL -- conform,
      # labels, features, assembly -- dies on a database it never reads.
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/default",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${replace(var.service, "-", "_")}/*",
    ]
  }

  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogStreams",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless/${var.service}:*"]
  }
}

resource "aws_iam_role_policy" "emr" {
  name   = "lake-access"
  role   = aws_iam_role.emr.id
  policy = data.aws_iam_policy_document.emr.json
}

###############################################################################
# SageMaker — drawio `sm` (Training) and `reg` (Model Registry)
#
# A role and a model package group, not a training job: a job is a run, and
# runs are launched by scripts/30_train.sh. There is deliberately no SageMaker
# ENDPOINT -- nothing is served. The registered model package IS the
# deliverable; what would consume it is drawn in docs/live_inference.drawio.
#
# There is also no managed MLflow tracking server (~$460/mo, README section 2).
# The model package group is the registry.
###############################################################################
data "aws_iam_policy_document" "sagemaker_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["sagemaker.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sagemaker" {
  name               = "${var.service}-sagemaker"
  assume_role_policy = data.aws_iam_policy_document.sagemaker_assume.json

  tags = {
    step = "training"
  }
}

data "aws_iam_policy_document" "sagemaker" {
  statement {
    sid       = "ReadTrainingData"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [data.aws_s3_bucket.model.arn, "${data.aws_s3_bucket.model.arn}/*"]
  }

  statement {
    sid = "WriteModelArtifacts"
    actions = [
      "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${data.aws_s3_bucket.model.arn}/models/*"]
  }

  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogGroup", "logs:CreateLogStream",
      "logs:PutLogEvents", "logs:DescribeLogStreams",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/sagemaker/*"]
  }

  statement {
    sid       = "PublishMetrics"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "cloudwatch:namespace"
      values   = ["/aws/sagemaker/TrainingJobs"]
    }
  }

  statement {
    sid = "RegisterModel"
    actions = [
      "sagemaker:CreateModelPackage", "sagemaker:DescribeModelPackage",
      "sagemaker:ListModelPackages", "sagemaker:UpdateModelPackage",
    ]
    resources = ["arn:${data.aws_partition.current.partition}:sagemaker:${var.aws_region}:${data.aws_caller_identity.current.account_id}:model-package/${var.service}*"]
  }
}

resource "aws_iam_role_policy" "sagemaker" {
  name   = "training-access"
  role   = aws_iam_role.sagemaker.id
  policy = data.aws_iam_policy_document.sagemaker.json
}

resource "aws_sagemaker_model_package_group" "registry" {
  model_package_group_name        = var.service
  model_package_group_description = "net_flow regressor + is_empty classifier, versioned with features.yaml and the fitted transformers (pipelines.md 6.8)"

  tags = {
    step = "training"
  }
}
