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

variable "emr_release_label" {
  description = "EMR Serverless release. 7.x carries Spark 3.5."
  type        = string
  default     = "emr-7.5.0"
}

variable "driver_cpu" {
  description = "Spark driver vCPU. README section 2 sizes this at 4."
  type        = string
  default     = "4 vCPU"
}

variable "driver_memory" {
  description = "Spark driver memory. Step 3.1's global bike_id sort is the peak-memory stage."
  type        = string
  default     = "16 GB"
}

variable "executor_cpu" {
  description = "Spark executor vCPU"
  type        = string
  default     = "4 vCPU"
}

variable "executor_memory" {
  description = "Spark executor memory"
  type        = string
  default     = "16 GB"
}

variable "executor_count" {
  description = "Maximum concurrent executors. Bounded by max_concurrent_vcpu: one driver plus this many executors, at 4 vCPU each, must fit inside the account quota."
  type        = number
  default     = 2
}

variable "max_concurrent_vcpu" {
  description = "The account's EMR Serverless 'Max concurrent vCPUs per account' quota (L-D05C8A75). Default for a new account is 16. Exceeding it does NOT fail at apply time -- the application is created happily and then every job dies minutes in with ServiceQuotaExceededException while requesting executors, which is a far more expensive way to find out."
  type        = number
  default     = 16
}

variable "training_instance_type" {
  description = "SageMaker training instance. README section 2: one ml.m5.4xlarge, ~$0.25/run."
  type        = string
  default     = "ml.m5.4xlarge"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "data_bucket_name" {
  description = "Override the data bucket name. Defaults to the storage layer's convention."
  type        = string
  default     = null
}

variable "model_bucket_name" {
  description = "Override the model bucket name. Same reasoning as data_bucket_name."
  type        = string
  default     = null
}
