output "emr_application_id" {
  description = "EMR Serverless application id. scripts/2x_*.sh submit jobs against this."
  value       = aws_emrserverless_application.spark.id
}

output "emr_job_role_arn" {
  description = "Role EMR Serverless jobs assume"
  value       = aws_iam_role.emr.arn
}

output "sagemaker_role_arn" {
  description = "Role SageMaker training jobs assume"
  value       = aws_iam_role.sagemaker.arn
}

output "model_package_group" {
  description = "SageMaker Model Registry group — the Pipeline 1 to Pipeline 2 handoff"
  value       = aws_sagemaker_model_package_group.registry.model_package_group_name
}

output "training_instance_type" {
  description = "Instance the training script requests"
  value       = var.training_instance_type
}
