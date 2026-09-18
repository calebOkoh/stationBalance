###############################################################################
# Lake layer — the two S3 zones the architecture draws, the read-only Glue
# catalog over them, the Athena workgroup that runs the QA gates, and the cost
# guardrail.
#
# Two buckets exactly, matching docs/aws_architecture.drawio: `s3raw` carries
# the immutable landing zone plus the intermediate Parquet, `s3gold` carries
# the modelling table and everything produced from it. No third bucket: code,
# logs and query results are prefixes on the gold bucket rather than resources
# the diagram does not contain.
###############################################################################

data "aws_caller_identity" "current" {}

locals {
  # Bucket names are globally unique, so the account id is the suffix that
  # makes this module re-deployable into a second account without edits.
  raw_bucket  = "${var.service}-lake-${data.aws_caller_identity.current.account_id}"
  gold_bucket = "${var.service}-gold-${data.aws_caller_identity.current.account_id}"
}

###############################################################################
# Zone 1 — /raw (immutable), /bronze, /silver
###############################################################################
resource "aws_s3_bucket" "raw" {
  bucket = local.raw_bucket

  tags = {
    step = "ingestion"
    zone = "raw-bronze-silver"
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

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
resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket     = aws_s3_bucket.raw.id
  depends_on = [aws_s3_bucket_versioning.raw]

  # The poller writes ~288 objects/day forever. Intelligent-Tiering costs
  # nothing to evaluate at this object size and stops the bill drifting as the
  # log grows without bound (README section 3).
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

  # bronze/ and silver/ are derived: any Spark re-run reproduces them, so old
  # versions are pure cost.
  rule {
    id     = "derived-expire-noncurrent"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }

  rule {
    id     = "silver-expire-noncurrent"
    status = "Enabled"

    filter {
      prefix = "silver/"
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
# Zone 2 — /gold (modelling table) plus the artifacts produced from it
###############################################################################
resource "aws_s3_bucket" "gold" {
  bucket = local.gold_bucket

  tags = {
    step = "assembly"
    zone = "gold"
  }
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket = aws_s3_bucket.gold.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

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
resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "gold" {
  bucket     = aws_s3_bucket.gold.id
  depends_on = [aws_s3_bucket_versioning.gold]

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
    id     = "gold-expire-noncurrent"
    status = "Enabled"

    filter {
      prefix = "gold/"
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
