output "api_endpoint" {
  description = "API Gateway origin. Usually reached through CloudFront instead."
  value       = aws_apigatewayv2_api.public.api_endpoint
}

output "public_url" {
  description = "The public entry point — CloudFront in front of the HTTP API"
  value       = "https://${aws_cloudfront_distribution.api.domain_name}"
}

output "routes" {
  description = "Try these once a model bundle has been published"
  value = {
    stations    = "https://${aws_cloudfront_distribution.api.domain_name}/stations"
    forecast    = "https://${aws_cloudfront_distribution.api.domain_name}/forecast?station_id=3005"
    attribution = "https://${aws_cloudfront_distribution.api.domain_name}/attribution"
  }
}

output "table_name" {
  description = "DynamoDB table holding the precomputed cube and the live snapshot"
  value       = aws_dynamodb_table.cube.name
}

output "inference_function_name" {
  value       = aws_lambda_function.inference.function_name
  description = "Invoke manually to regenerate the cube without waiting for the schedule"
}

output "schedules_enabled" {
  description = "False until a model bundle exists; flip var.enable_inference after the first training run"
  value       = var.enable_inference
}
