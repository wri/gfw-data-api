output "aurora_job_definition" {
  value = aws_batch_job_definition.aurora.arn
}

output "aurora_job_queue" {
  value = aws_batch_job_queue.aurora.arn
}

output "aurora_job_queue_fast" {
  value = aws_batch_job_queue.aurora_fast.arn
}

output "data_lake_job_definition" {
  value = aws_batch_job_definition.data_lake.arn
}

output "data_lake_job_queue" {
  value = aws_batch_job_queue.data_lake.arn
}

output "tile_cache_job_definition" {
  value = aws_batch_job_definition.tile_cache.arn
}

output "tile_cache_job_queue" {
  value = aws_batch_job_queue.tile_cache.arn
}