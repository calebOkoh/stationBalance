###############################################################################
# EventBridge Scheduler — drawio `eb` ("5 min | hourly | daily").
#
# EventBridge Scheduler rather than classic CloudWatch Events rules: one
# resource per schedule, a real retry policy, and the flexible time window that
# spreads the daily snapshots off the top of the hour. The first 14 M
# invocations a month are free, and this is ~290/day.
#
# Three recurring schedules, and only three. Everything else in Pipeline 1 is
# attended -- .claude/decisions.md cut Step Functions because "every run is
# attended" and numbered scripts give the same ordering with better
# debuggability. The three that DO recur are the three whose data cannot be
# backfilled:
#
#   poller        5 min   station_status has no published archive at all
#   station_info  daily   capacity changes, and no history of it is published
#   closures      daily   the ArcGIS layer PURGES expired permits
#
# Each is a source that silently loses depth for every interval it is not
# running. Nothing else here is on a timer.
###############################################################################

data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    # Confused-deputy guard: without this, any account whose scheduler can
    # name this role ARN could invoke through it.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.service}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json

  tags = {
    step = "ingestion"
  }
}

data "aws_iam_policy_document" "scheduler_invoke" {
  statement {
    sid     = "InvokeCollectors"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.poller.arn,
      aws_lambda_function.ingest.arn,
    ]
  }
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name   = "invoke-collectors"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_invoke.json
}

###############################################################################
# 0.5 — station_status poller, every 5 minutes, from day one
###############################################################################
resource "aws_scheduler_schedule" "poller" {
  name       = "${var.service}-station-status-poll"
  group_name = "default"
  state      = var.enable_collectors ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = "rate(${var.poll_interval_minutes} minutes)"
  schedule_expression_timezone = "UTC"

  target {
    arn      = aws_lambda_function.poller.arn
    role_arn = aws_iam_role.scheduler.arn

    # A missed poll is a permanent five-minute hole in the validation set, so
    # it is worth retrying -- but not past the next scheduled poll, which would
    # only duplicate work.
    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 240
    }
  }
}

###############################################################################
# 1.3 — station_information snapshot, daily
#
# Capacity changes and Indego publishes no history of it, so this dated
# snapshot IS the record of when each value was observed. Step 3.7 cross-checks
# its rolling-90-day capacity estimate against these.
###############################################################################
resource "aws_scheduler_schedule" "station_info" {
  name       = "${var.service}-station-info-snapshot"
  group_name = "default"
  state      = var.enable_collectors ? "ENABLED" : "DISABLED"

  # An hour of jitter. Nothing downstream cares when in the day the snapshot
  # lands, and spreading it avoids hammering the feed on the hour.
  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 60
  }

  schedule_expression          = "cron(15 7 * * ? *)"
  schedule_expression_timezone = "America/New_York"

  target {
    arn      = aws_lambda_function.ingest.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ task = "station_info" })

    retry_policy {
      maximum_retry_attempts       = 3
      maximum_event_age_in_seconds = 3600
    }
  }
}

###############################################################################
# 1.5 — closure layer snapshot, daily
#
# The LaneClosure_Master layer is current-state and expired permits ARE purged
# (verified 2026-09-17: only 1 / 2 / 15 permits survive from 2022 / 2023 /
# 2024). Without a dated snapshot its historical depth erodes silently. This is
# the inference-path closure source; the training history comes from the PGW
# backfill, which is one-time and runs from scripts/pgw_backfill.py.
###############################################################################
resource "aws_scheduler_schedule" "closures" {
  name       = "${var.service}-closure-snapshot"
  group_name = "default"
  state      = var.enable_collectors ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 60
  }

  schedule_expression          = "cron(45 7 * * ? *)"
  schedule_expression_timezone = "America/New_York"

  target {
    arn      = aws_lambda_function.ingest.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({ task = "closures" })

    retry_policy {
      maximum_retry_attempts       = 3
      maximum_event_age_in_seconds = 3600
    }
  }
}
