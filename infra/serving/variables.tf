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
  type    = number
  default = 30
}

variable "horizon_hours" {
  description = "Forecast depth per station. 48h x ~250 stations is the ~12 K-row cube README section 2 sizes."
  type        = number
  default     = 48
}

variable "inference_schedule" {
  description = "How often the cube is regenerated. Hourly: the weather forecast underneath it does not update faster, so anything more frequent is spend without new information."
  type        = string
  default     = "rate(1 hour)"
}

variable "status_refresh_schedule" {
  description = "How often live dock counts are refreshed. drawio edge e26 draws 60s."
  type        = string
  default     = "rate(1 minute)"
}

variable "enable_inference" {
  description = "Whether the two Pipeline 2 schedules are live. Leave false until a model bundle exists in s3://<gold>/models/current/ -- the inference Lambda fails without one."
  type        = bool
  default     = false
}

variable "cors_allow_origins" {
  description = "Origins permitted to call the HTTP API. The web tool is out of scope for this repo, so this defaults open for read-only public data."
  type        = list(string)
  default     = ["*"]
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
