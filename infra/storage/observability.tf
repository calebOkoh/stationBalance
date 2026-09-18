###############################################################################
# Observability and the cost guardrail (drawio `cw`).
#
# README section 2: "Every meaningful cost risk is something *left running*: a
# NAT Gateway ($32/mo), a managed MLflow tracking server (~$460/mo), or
# pre-initialised EMR capacity." None of those are provisioned here, so the
# budget exists to catch the one that gets added by hand later.
###############################################################################
resource "aws_budgets_budget" "monthly" {
  name         = "${var.service}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:service$${var.service}"]
  }

  tags = {
    step = "observability"
  }

  # Forecast-based, so it fires before the money is spent rather than after.
  dynamic "notification" {
    for_each = var.budget_alert_email == "" ? [] : [1]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = 80
      threshold_type             = "PERCENTAGE"
      notification_type          = "FORECASTED"
      subscriber_email_addresses = [var.budget_alert_email]
    }
  }

  dynamic "notification" {
    for_each = var.budget_alert_email == "" ? [] : [1]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = 100
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.budget_alert_email]
    }
  }
}

# Created here rather than letting EMR create them implicitly, so retention is
# bounded. An unbounded log group is a slow leak, not an outage, which is
# exactly the kind of cost that goes unnoticed.
resource "aws_cloudwatch_log_group" "emr" {
  name              = "/aws/emr-serverless/${var.service}"
  retention_in_days = var.log_retention_days

  tags = {
    step = "processing"
  }
}
