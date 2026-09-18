variable "aws_region" {
  description = "Target region. Everything lives in one region; there is no cross-region path."
  type        = string
  default     = "us-east-1"
}

variable "service" {
  description = "Value of the `service` tag applied to every taggable resource in the project"
  type        = string
  default     = "station-balance"
}

variable "log_retention_days" {
  description = "CloudWatch log retention for the ingest Lambda"
  type        = number
  default     = 30
}


variable "data_bucket_name" {
  description = "Override the data bucket name. Defaults to the storage layer's convention; set it only when pointing this layer at buckets named differently."
  type        = string
  default     = null
}
