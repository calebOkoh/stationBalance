output "data_bucket" {
  description = "Downloaded archives and everything derived from them: raw/, parsed/, clean/"
  value       = aws_s3_bucket.data.id
}

output "model_bucket" {
  description = "Training splits and model artifacts: training/, models/, code/, athena-results/, emr-logs/"
  value       = aws_s3_bucket.model.id
}

output "glue_database" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.catalog.name
}

output "athena_workgroup" {
  description = "Athena workgroup that runs the QA gates"
  value       = aws_athena_workgroup.qa.name
}

output "emr_log_group" {
  description = "CloudWatch log group EMR Serverless writes to"
  value       = aws_cloudwatch_log_group.emr.name
}

output "features_json_uri" {
  description = "features.yaml rendered to JSON — what the Spark jobs and the training job read"
  value       = "s3://${aws_s3_bucket.model.id}/${aws_s3_object.features_json.key}"
}
