###############################################################################
# Storage layer — the two S3 buckets the architecture draws, the read-only Glue
# catalog over them, the Athena workgroup that runs the QA gates, and the cost
# guardrail.
#
# Two buckets, matching docs/pretrained_model.drawio: one holds the downloaded
# archives and everything derived from them, the other holds the training
# splits and the model artifacts. Code, logs and query results are prefixes on
# the model bucket rather than a third bucket.
###############################################################################

data "aws_caller_identity" "current" {}

locals {
  # Bucket names are globally unique, so the account id is the suffix that
  # makes this module re-deployable into a second account without edits.
  data_bucket  = "${var.service}-data-${data.aws_caller_identity.current.account_id}"
  model_bucket = "${var.service}-model-${data.aws_caller_identity.current.account_id}"
}

###############################################################################
# Data bucket — raw/ (immutable), parsed/, clean/
###############################################################################
resource "aws_s3_bucket" "data" {
  bucket = local.data_bucket

  tags = {
    step = "ingestion"
    zone = "raw-parsed-clean"
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# /raw is the immutable source of record: "download once, never re-fetch, so
# results reproduce even if Indego revises a file" (pipelines.md 1.1).
# Versioning is what actually enforces that, since an accidental overwrite is
# otherwise unrecoverable.
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket     = aws_s3_bucket.data.id
  depends_on = [aws_s3_bucket_versioning.data]

  # Intelligent-Tiering costs nothing to evaluate at this object size, and
  # raw/ is written once and then read a handful of times per pipeline run --
  # exactly the access pattern it exists for.
  rule {
    id     = "raw-intelligent-tiering"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 0
      storage_class = "INTELLIGENT_TIERING"
    }
  }

  # parsed/ and clean/ are derived: any Spark re-run reproduces them, so old
  # versions are pure cost.
  rule {
    id     = "parsed-expire-noncurrent"
    status = "Enabled"

    filter {
      prefix = "parsed/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "clean-expire-noncurrent"
    status = "Enabled"

    filter {
      prefix = "clean/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

###############################################################################
# Model bucket — training/ (splits) plus the artifacts produced from them
###############################################################################
resource "aws_s3_bucket" "model" {
  bucket = local.model_bucket

  tags = {
    step = "assembly"
    zone = "training-models"
  }
}

resource "aws_s3_bucket_public_access_block" "model" {
  bucket = aws_s3_bucket.model.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "model" {
  bucket = aws_s3_bucket.model.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Frozen splits and exported model bundles are the artifacts every reported
# number is traceable to. Versioning is the cheap insurance against an
# experiment overwriting the run that produced the deliverable.
resource "aws_s3_bucket_versioning" "model" {
  bucket = aws_s3_bucket.model.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "model" {
  bucket     = aws_s3_bucket.model.id
  depends_on = [aws_s3_bucket_versioning.model]

  # Athena result sets and EMR logs are debris. Neither is worth storing past
  # the run that produced it.
  rule {
    id     = "expire-athena-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 14
    }
  }

  rule {
    id     = "expire-emr-logs"
    status = "Enabled"

    filter {
      prefix = "emr-logs/"
    }

    expiration {
      days = 30
    }
  }

  rule {
    id     = "training-expire-noncurrent"
    status = "Enabled"

    filter {
      prefix = "training/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}
