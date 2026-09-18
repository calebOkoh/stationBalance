###############################################################################
# Pipeline 2 schedules — drawio edge `e31` (EventBridge -> inference) and the
# 60-second cadence on `e26`.
#
# Both default to DISABLED. The inference Lambda reads a model bundle from
# s3://<gold>/models/current/; until phase 6 has exported one, an enabled
# schedule is an hourly error. Flip `enable_inference` after the first
# successful training run -- scripts/40_publish_model.sh prints the reminder.
###############################################################################
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.service}-serving-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json

  tags = {
    step = "serving"
  }
}

data "aws_iam_policy_document" "scheduler_invoke" {
  statement {
    sid     = "InvokePipeline2"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.inference.arn,
      aws_lambda_function.status_refresh.arn,
    ]
  }
}

resource "aws_iam_role_policy" "scheduler_invoke" {
  name   = "invoke-pipeline-2"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler_invoke.json
}

# Hourly. The weather forecast underneath the cube does not update faster, so a
# tighter cadence would spend money to recompute the same answer.
resource "aws_scheduler_schedule" "inference" {
  name       = "${var.service}-inference-refresh"
  group_name = "default"
  state      = var.enable_inference ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 5
  }

  schedule_expression = var.inference_schedule

  target {
    arn      = aws_lambda_function.inference.arn
    role_arn = aws_iam_role.scheduler.arn

    # A failed refresh degrades to stale data rather than a 5xx (README
    # section 2), so one retry is enough; the next hour's run recovers anyway.
    retry_policy {
      maximum_retry_attempts       = 1
      maximum_event_age_in_seconds = 900
    }
  }
}

# Every 60 s — what keeps the map dots live.
resource "aws_scheduler_schedule" "status_refresh" {
  name       = "${var.service}-status-refresh"
  group_name = "default"
  state      = var.enable_inference ? "ENABLED" : "DISABLED"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = var.status_refresh_schedule

  target {
    arn      = aws_lambda_function.status_refresh.arn
    role_arn = aws_iam_role.scheduler.arn

    # No retry: the next run is 60 seconds away and carries fresher data than
    # a retry of the failed one would.
    retry_policy {
      maximum_retry_attempts = 0
    }
  }
}
