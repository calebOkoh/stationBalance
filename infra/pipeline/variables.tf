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
  description = "Maximum concurrent executors. README section 2 sizes this at 4."
  type        = number
  default     = 4
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
