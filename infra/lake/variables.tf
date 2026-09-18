variable "aws_region" {
  description = "Target region. Everything lives in one region; there is no cross-region path."
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Named AWS profile used for this layer. Must be able to create IAM roles and attach policies."
  type        = string
  default     = "coa-dev"
}

variable "service" {
  description = "Value of the `service` tag applied to every taggable resource in the project"
  type        = string
  default     = "station-balance"
}

variable "log_retention_days" {
  description = "CloudWatch log retention. Logs are for debugging an attended run, not an audit trail — keep them short."
  type        = number
  default     = 30
}

variable "monthly_budget_usd" {
  description = "Monthly cost ceiling. README section 2 puts steady state under $5/month; this alarms well before a NAT Gateway or pre-initialised EMR capacity could go unnoticed."
  type        = number
  default     = 25
}

variable "budget_alert_email" {
  description = "Address that receives the budget alarm. Empty disables the notification (the budget itself is still created)."
  type        = string
  default     = ""
}
