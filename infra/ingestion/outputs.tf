output "ingest_function_name" {
  description = "Invoke with {\"task\": \"trips\"|\"stations\"|\"station_info\"|\"closures\"|\"closures_bulk\"|\"geo\"|\"weather\"}"
  value       = aws_lambda_function.ingest.function_name
}

output "poller_function_name" {
  description = "station_status poller. Running means the validation set is accruing."
  value       = aws_lambda_function.poller.function_name
}

output "raw_bucket" {
  description = "Landing zone this layer writes to"
  value       = data.aws_s3_bucket.raw.id
}

output "collector_schedules" {
  description = "The only three recurring schedules in the project"
  value = {
    station_status = aws_scheduler_schedule.poller.schedule_expression
    station_info   = aws_scheduler_schedule.station_info.schedule_expression
    closures       = aws_scheduler_schedule.closures.schedule_expression
  }
}
