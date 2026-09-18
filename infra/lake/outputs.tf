output "raw_bucket" {
  description = "Landing zone bucket: /raw (immutable), /bronze, /silver"
  value       = aws_s3_bucket.raw.id
}

output "gold_bucket" {
  description = "Modelling bucket: /gold, /models, /code, /athena-results, /emr-logs"
  value       = aws_s3_bucket.gold.id
}

output "glue_database" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.lake.name
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
  value       = "s3://${aws_s3_bucket.gold.id}/${aws_s3_object.features_json.key}"
}
