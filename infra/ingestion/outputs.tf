output "ingest_function_name" {
  description = "Invoke with {\"task\": \"trips\"|\"stations\"|\"weather\"}. scripts/10_ingest.sh does this for you."
  value       = aws_lambda_function.ingest.function_name
}

output "data_bucket" {
  description = "Bucket this layer writes downloaded archives into, under raw/"
  value       = data.aws_s3_bucket.data.id
}
