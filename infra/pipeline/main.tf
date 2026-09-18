###############################################################################
# Pipeline layer — the batch compute the architecture draws: EMR Serverless for
# phases 2-5, SageMaker for phase 6, and the model package group that is the
# handoff to Pipeline 2.
#
# Nothing in this layer runs on a schedule. Pipeline 1 "runs a handful of
# times, attended" (.claude/decisions.md), so ordering comes from the numbered
# scripts in scripts/ rather than from Step Functions. What Terraform owns here
# is the capacity to run a phase, not the decision to run one.
###############################################################################

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

data "aws_s3_bucket" "raw" {
  bucket = coalesce(
    var.raw_bucket_name,
    "${var.service}-lake-${data.aws_caller_identity.current.account_id}",
  )
}

data "aws_s3_bucket" "gold" {
  bucket = coalesce(
    var.gold_bucket_name,
    "${var.service}-gold-${data.aws_caller_identity.current.account_id}",
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

  # A hard ceiling, not a target. Phases 2-5 operate on a ~10 M-row grid that
  # fits in memory; this exists so a runaway job cannot scale into real money.
  maximum_capacity {
    cpu    = "${(var.executor_count + 1) * 4} vCPU"
    memory = "${(var.executor_count + 1) * 16} GB"
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
  # Reads /raw, writes /bronze and /silver. It does NOT get write access to
  # raw/ -- that zone is immutable by design (pipelines.md 0.1), and a Spark
  # job with a bad output path is exactly how that invariant gets broken.
  statement {
    sid       = "ReadRawZone"
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_s3_bucket.raw.arn}/raw/*"]
  }

  statement {
    sid = "WriteDerivedZones"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = [
      "${data.aws_s3_bucket.raw.arn}/bronze/*",
      "${data.aws_s3_bucket.raw.arn}/silver/*",
      "${data.aws_s3_bucket.gold.arn}/gold/*",
      "${data.aws_s3_bucket.gold.arn}/models/*",
      "${data.aws_s3_bucket.gold.arn}/emr-logs/*",
    ]
  }

  statement {
    sid       = "ReadJobCode"
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_s3_bucket.gold.arn}/code/*"]
  }

  statement {
    sid       = "ListBuckets"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [data.aws_s3_bucket.raw.arn, data.aws_s3_bucket.gold.arn]
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
    ]
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/${replace(var.service, "-", "_")}",
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
# ENDPOINT anywhere in this project -- the model is not on the request path,
# inference is a Lambda writing a precomputed cube, and a warm endpoint would
# be the second-largest line on the bill after a NAT Gateway.
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
    resources = [data.aws_s3_bucket.gold.arn, "${data.aws_s3_bucket.gold.arn}/*"]
  }

  statement {
    sid = "WriteModelArtifacts"
    actions = [
      "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
    ]
    resources = ["${data.aws_s3_bucket.gold.arn}/models/*"]
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
