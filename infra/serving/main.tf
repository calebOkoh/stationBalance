###############################################################################
# Serving layer — Pipeline 2 as drawn: the inference Lambda that writes the
# precomputed cube, the status refresher that keeps the map dots live, and the
# DynamoDB table both write into.
#
# The two schedules here are OFF by default (`enable_inference`). The inference
# Lambda needs a model bundle at s3://<gold>/models/current/, which only exists
# after phase 6 has run. Shipping them enabled would mean a Lambda erroring
# every hour from the moment of apply.
###############################################################################

data "aws_caller_identity" "current" {}

data "aws_s3_bucket" "gold" {
  bucket = coalesce(
    var.gold_bucket_name,
    "${var.service}-gold-${data.aws_caller_identity.current.account_id}",
  )
}

locals {
  build_dir = "${path.module}/../../build/lambdas"
}

###############################################################################
# DynamoDB — drawio `ddb`
#
# Key design is the one the diagram names: `SNAPSHOT#CURRENT` / `STATION#id`
# and `STATION#id` / `FORECAST#ts`. One table, two access patterns:
#
#   the whole live map   GetItem  pk=SNAPSHOT#CURRENT, sk=ALL
#   one tendency graph   Query    pk=STATION#<id>, sk begins_with FORECAST#
#
# PAY_PER_REQUEST because the load is ~12 K writes/hour and a handful of reads.
# Provisioned capacity would mean paying for a floor that is never approached.
###############################################################################
resource "aws_dynamodb_table" "cube" {
  name         = var.service
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  # Expiry is how a dead refresher becomes visible. Without it the API would
  # serve last week's numbers as if they were current.
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  # Every row is regenerated from S3 by the next inference run, so there is
  # nothing here that point-in-time recovery could restore that a re-run
  # cannot. Leaving it off is the honest call, not a corner cut.
  point_in_time_recovery {
    enabled = false
  }

  tags = {
    step = "serving"
  }
}

###############################################################################
# Packaging
###############################################################################
data "archive_file" "inference" {
  type        = "zip"
  source_dir  = "${local.build_dir}/inference"
  output_path = "${local.build_dir}/inference.zip"
}

data "archive_file" "status_refresh" {
  type        = "zip"
  source_dir  = "${local.build_dir}/status_refresh"
  output_path = "${local.build_dir}/status_refresh.zip"
}

data "archive_file" "api" {
  type        = "zip"
  source_dir  = "${local.build_dir}/api"
  output_path = "${local.build_dir}/api.zip"
}

###############################################################################
# IAM
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

resource "aws_iam_role" "inference" {
  name               = "${var.service}-inference"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json

  tags = {
    step = "inference"
  }
}

resource "aws_iam_role" "status_refresh" {
  name               = "${var.service}-status-refresh"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json

  tags = {
    step = "serving"
  }
}

resource "aws_iam_role" "api" {
  name               = "${var.service}-api"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json

  tags = {
    step = "serving"
  }
}

data "aws_iam_policy_document" "inference" {
  statement {
    sid       = "ReadArtifactBundle"
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_s3_bucket.gold.arn}/models/*"]
  }

  statement {
    sid       = "WriteCube"
    actions   = ["dynamodb:PutItem", "dynamodb:BatchWriteItem"]
    resources = [aws_dynamodb_table.cube.arn]
  }
}

data "aws_iam_policy_document" "status_refresh" {
  statement {
    sid       = "WriteSnapshot"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.cube.arn]
  }
}

data "aws_iam_policy_document" "api" {
  # Read-only, and no Scan: a Scan over the cube would read ~12 K items to
  # answer a question about one station.
  statement {
    sid       = "ReadCube"
    actions   = ["dynamodb:GetItem", "dynamodb:Query"]
    resources = [aws_dynamodb_table.cube.arn]
  }

  statement {
    sid       = "ReadAttribution"
    actions   = ["s3:GetObject"]
    resources = ["${data.aws_s3_bucket.gold.arn}/models/*"]
  }
}

resource "aws_iam_role_policy" "inference" {
  name   = "inference-access"
  role   = aws_iam_role.inference.id
  policy = data.aws_iam_policy_document.inference.json
}

resource "aws_iam_role_policy" "status_refresh" {
  name   = "snapshot-write"
  role   = aws_iam_role.status_refresh.id
  policy = data.aws_iam_policy_document.status_refresh.json
}

resource "aws_iam_role_policy" "api" {
  name   = "cube-read"
  role   = aws_iam_role.api.id
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_iam_role_policy_attachment" "inference_logs" {
  role       = aws_iam_role.inference.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "status_refresh_logs" {
  role       = aws_iam_role.status_refresh.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "api_logs" {
  role       = aws_iam_role.api.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

###############################################################################
# Lambdas — drawio `inf`, `stat`, plus the API reader
###############################################################################
resource "aws_cloudwatch_log_group" "inference" {
  name              = "/aws/lambda/${var.service}-inference"
  retention_in_days = var.log_retention_days
  tags              = { step = "inference" }
}

resource "aws_cloudwatch_log_group" "status_refresh" {
  name              = "/aws/lambda/${var.service}-status-refresh"
  retention_in_days = var.log_retention_days
  tags              = { step = "serving" }
}

resource "aws_cloudwatch_log_group" "api" {
  name              = "/aws/lambda/${var.service}-api"
  retention_in_days = var.log_retention_days
  tags              = { step = "serving" }
}

resource "aws_lambda_function" "inference" {
  function_name = "${var.service}-inference"
  role          = aws_iam_role.inference.arn
  handler       = "handler.handler"
  runtime       = "python3.13"
  architectures = ["arm64"]

  filename         = data.archive_file.inference.output_path
  source_code_hash = data.archive_file.inference.output_base64sha256

  # ~12 K rows x two tree walks, plus three upstream calls. Memory is the lever
  # that matters: Lambda scales CPU with it, and the tree traversal is pure
  # Python, so 2 GB finishes several times faster than 512 MB for very nearly
  # the same GB-second cost.
  timeout     = 600
  memory_size = 2048

  environment {
    variables = {
      GOLD_BUCKET   = data.aws_s3_bucket.gold.id
      TABLE_NAME    = aws_dynamodb_table.cube.name
      BUNDLE_PREFIX = "models/current"
      HORIZON_HOURS = tostring(var.horizon_hours)
      LOG_LEVEL     = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.inference]
  tags       = { step = "inference" }
}

resource "aws_lambda_function" "status_refresh" {
  function_name = "${var.service}-status-refresh"
  role          = aws_iam_role.status_refresh.arn
  handler       = "handler.handler"
  runtime       = "python3.13"
  architectures = ["arm64"]

  filename         = data.archive_file.status_refresh.output_path
  source_code_hash = data.archive_file.status_refresh.output_base64sha256

  timeout     = 30
  memory_size = 256

  environment {
    variables = {
      TABLE_NAME = aws_dynamodb_table.cube.name
      LOG_LEVEL  = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.status_refresh]
  tags       = { step = "serving" }
}

resource "aws_lambda_function" "api" {
  function_name = "${var.service}-api"
  role          = aws_iam_role.api.arn
  handler       = "handler.handler"
  runtime       = "python3.13"
  architectures = ["arm64"]

  filename         = data.archive_file.api.output_path
  source_code_hash = data.archive_file.api.output_base64sha256

  # One DynamoDB read. The budget here is cold-start, not compute.
  timeout     = 10
  memory_size = 512

  environment {
    variables = {
      TABLE_NAME    = aws_dynamodb_table.cube.name
      GOLD_BUCKET   = data.aws_s3_bucket.gold.id
      BUNDLE_PREFIX = "models/current"
    }
  }

  depends_on = [aws_cloudwatch_log_group.api]
  tags       = { step = "serving" }
}
