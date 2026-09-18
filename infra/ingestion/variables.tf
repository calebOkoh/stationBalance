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
  description = "CloudWatch log retention for the collector Lambdas"
  type        = number
  default     = 30
}

variable "poll_interval_minutes" {
  description = "station_status poll interval. 5 minutes is the value pipelines.md 0.5 pins; the feed's own ttl is ~60s, so this is already conservative."
  type        = number
  default     = 5
}

variable "enable_collectors" {
  description = "Master switch for the three recurring schedules. The poller in particular should stay on -- its data cannot be backfilled."
  type        = bool
  default     = true
}

variable "raw_bucket_name" {
  description = "Override the raw-zone bucket name. Defaults to the lake layer's convention; set it only when pointing this layer at a lake that was named differently."
  type        = string
  default     = null
}

variable "gold_bucket_name" {
  description = "Override the gold bucket name. Same reasoning as raw_bucket_name."
  type        = string
  default     = null
}
