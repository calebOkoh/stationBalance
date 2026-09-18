###############################################################################
# Ingestion layer — one Lambda that downloads the historical archives.
#
# There is NO schedule here, and no EventBridge resource anywhere in this
# project. Every input is a published archive fetched once: the quarterly trip
# ZIPs, the station table, and the Open-Meteo historical range. Nothing polls,
# because nothing live is used — live feeds belong to the inference
# architecture in docs/live_inference.drawio, which is not built.
#
# The function is invoked by hand through scripts/10_ingest.sh.
###############################################################################

data "aws_caller_identity" "current" {}

# Looked up by name rather than read from the lake layer's state, so the two
# stay decoupled. Apply infra/storage first or these lookups fail.
data "aws_s3_bucket" "data" {
  bucket = coalesce(
    var.data_bucket_name,
    "${var.service}-data-${data.aws_caller_identity.current.account_id}",
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

###############################################################################
# IAM
#
# Scoped to raw/ and nothing else. The function's whole job is landing
# downloads; granting it the derived prefixes would let a bad task overwrite
# Spark output that took an hour to produce.
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

data "aws_iam_policy_document" "ingest" {
  statement {
    sid       = "WriteRawPrefix"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${data.aws_s3_bucket.data.arn}/raw/*"]
  }

  # HeadObject is what makes re-invocation cheap: /raw is immutable, so the
  # ingest tasks skip anything already landed rather than re-fetching it.
  statement {
    sid       = "ListRawZone"
    actions   = ["s3:ListBucket"]
    resources = [data.aws_s3_bucket.data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["raw/*"]
    }
  }
}

resource "aws_iam_role_policy" "ingest" {
  name   = "s3-raw-access"
  role   = aws_iam_role.ingest.id
  policy = data.aws_iam_policy_document.ingest.json
}

resource "aws_iam_role_policy_attachment" "ingest_logs" {
  role       = aws_iam_role.ingest.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

###############################################################################
# Lambda — drawio `lam`, the single ingest function
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

resource "aws_lambda_function" "ingest" {
  function_name = "${var.service}-ingest"
  role          = aws_iam_role.ingest.arn
  handler       = "handler.handler"
  runtime       = "python3.13"
  architectures = ["arm64"]

  filename         = data.archive_file.ingest.output_path
  source_code_hash = data.archive_file.ingest.output_base64sha256

  # The weather task is the long one: ~4 years x 4 grid points in 6-month
  # chunks is ~40 sequential calls. 15 minutes is the Lambda ceiling and it
  # fits comfortably; trips and stations are seconds.
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
      DATA_BUCKET    = data.aws_s3_bucket.data.id
      WEATHER_CONFIG = local.weather_config
      LOG_LEVEL      = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.ingest]

  tags = {
    step = "ingestion"
  }
}
