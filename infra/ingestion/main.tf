###############################################################################
# Ingestion layer — the two collector Lambdas the architecture draws, their
# roles, and the EventBridge schedules that drive them.
#
# Applying this layer is what makes the project start collecting. The poller
# in particular begins on apply and never stops: there is no historical archive
# of dock occupancy, so every day it is not running is validation data that
# cannot be recovered (pipelines.md 0.5).
###############################################################################

data "aws_caller_identity" "current" {}

# Looked up by name rather than read from the lake layer's state, so the two
# stay decoupled. Apply infra/lake first or these lookups fail.
data "aws_s3_bucket" "raw" {
  bucket = coalesce(
    var.raw_bucket_name,
    "${var.service}-lake-${data.aws_caller_identity.current.account_id}",
  )
}

locals {
  build_dir = "${path.module}/../../build/lambdas"

  # features.yaml is the shared train/serve contract (pipelines.md 0.4), so it
  # is read here rather than duplicated into Terraform variables. `yamldecode`
  # means there is exactly one place the weather variable list lives, and a
  # change to it shows up as a Lambda environment diff at plan time.
  features = yamldecode(file("${path.module}/../../features.yaml"))

  # Handed to the Lambda as JSON so the function needs no YAML parser -- and
  # therefore no dependency layer, no wheel to rebuild, and no build step
  # beyond copying .py files.
  weather_config = jsonencode({
    start_date  = local.features.project.train_window_start
    timezone    = local.features.project.timezone
    hourly      = local.features.weather.hourly
    daily       = local.features.weather.daily
    grid_points = local.features.weather.grid_points
  })
}

###############################################################################
# Packaging
#
# The build directory is produced by scripts/build_lambdas.sh, which stages the
# handler alongside src/lambdas/common/. Terraform zips it here rather than
# shelling out, so `plan` is accurate and no `zip` binary is required.
###############################################################################
data "archive_file" "ingest" {
  type        = "zip"
  source_dir  = "${local.build_dir}/ingest"
  output_path = "${local.build_dir}/ingest.zip"
}

data "archive_file" "poller" {
  type        = "zip"
  source_dir  = "${local.build_dir}/poller"
  output_path = "${local.build_dir}/poller.zip"
}

###############################################################################
# IAM
#
# Both roles are scoped to the prefixes their function actually writes. The
# poller writes exactly one prefix; granting it the whole bucket would let a
# bug in a five-minute cron overwrite the immutable trip archives.
###############################################################################
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingest" {
  name               = "${var.service}-ingest"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json

  tags = {
    step = "ingestion"
  }
}

resource "aws_iam_role" "poller" {
  name               = "${var.service}-poller"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json

  tags = {
    step = "ingestion"
  }
}

data "aws_iam_policy_document" "ingest" {
  statement {
    sid       = "WriteRawZone"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${data.aws_s3_bucket.raw.arn}/raw/*"]
  }

  # HeadObject is what makes re-invocation cheap: /raw is immutable, so the
  # ingest tasks skip anything already landed rather than re-fetching it.
  statement {
    sid       = "ListRawZone"
    actions   = ["s3:ListBucket"]
    resources = [data.aws_s3_bucket.raw.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }
}

data "aws_iam_policy_document" "poller" {
  statement {
    sid       = "WriteStationStatusOnly"
    actions   = ["s3:PutObject"]
    resources = ["${data.aws_s3_bucket.raw.arn}/raw/station_status/*"]
  }
}

resource "aws_iam_role_policy" "ingest" {
  name   = "s3-raw-access"
  role   = aws_iam_role.ingest.id
  policy = data.aws_iam_policy_document.ingest.json
}

resource "aws_iam_role_policy" "poller" {
  name   = "s3-station-status-access"
  role   = aws_iam_role.poller.id
  policy = data.aws_iam_policy_document.poller.json
}

resource "aws_iam_role_policy_attachment" "ingest_logs" {
  role       = aws_iam_role.ingest.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "poller_logs" {
  role       = aws_iam_role.poller.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

###############################################################################
# Lambdas — drawio `g1` (archives | weather | geo) and `g3` (station_status)
#
# arm64 because Graviton is ~20% cheaper per GB-second for identical work, and
# these are pure-Python functions with no native wheels to worry about.
#
# Deliberately NOT in a VPC. README section 2: keeping Lambdas out of a VPC is
# what makes a NAT Gateway ($32/mo, an order of magnitude above the rest of the
# steady-state bill) unnecessary.
###############################################################################
resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${var.service}-ingest"
  retention_in_days = var.log_retention_days

  tags = {
    step = "ingestion"
  }
}

resource "aws_cloudwatch_log_group" "poller" {
  name              = "/aws/lambda/${var.service}-poller"
  retention_in_days = var.log_retention_days

  tags = {
    step = "ingestion"
  }
}

resource "aws_lambda_function" "ingest" {
  function_name = "${var.service}-ingest"
  role          = aws_iam_role.ingest.arn
  handler       = "handler.handler"
  runtime       = "python3.13"
  architectures = ["arm64"]

  filename         = data.archive_file.ingest.output_path
  source_code_hash = data.archive_file.ingest.output_base64sha256

  # The geo and weather tasks are the long ones: paging the street centerlines
  # and walking ~4 years x 4 grid points in 6-month chunks. 15 minutes is the
  # Lambda ceiling and these fit inside it; the trip archives are landed one
  # invocation per call so they never approach it.
  timeout     = 900
  memory_size = 1024

  # Lambda's CPU share scales with memory, so a larger /tmp is not the reason
  # for 1024 MB -- network throughput is. Nothing here holds a whole archive in
  # memory; bodies stream to S3.
  ephemeral_storage {
    size = 1024
  }

  environment {
    variables = {
      RAW_BUCKET     = data.aws_s3_bucket.raw.id
      WEATHER_CONFIG = local.weather_config
      LOG_LEVEL      = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.ingest]

  tags = {
    step = "ingestion"
  }
}

resource "aws_lambda_function" "poller" {
  function_name = "${var.service}-poller"
  role          = aws_iam_role.poller.arn
  handler       = "handler.handler"
  runtime       = "python3.13"
  architectures = ["arm64"]

  filename         = data.archive_file.poller.output_path
  source_code_hash = data.archive_file.poller.output_base64sha256

  # One HTTP GET and one PutObject of ~250 records. 128 MB is the floor and
  # this genuinely fits; at 288 invocations/day the cost is rounding error.
  timeout     = 60
  memory_size = 256

  environment {
    variables = {
      RAW_BUCKET = data.aws_s3_bucket.raw.id
      LOG_LEVEL  = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.poller]

  tags = {
    step = "ingestion"
  }
}
