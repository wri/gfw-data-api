output "aurora_job_definition" {
  value = aws_batch_job_definition.aurora
}

output "aurora_job_definition_arn" {
  value = aws_batch_job_definition.aurora.arn
}

output "aurora_job_queue_arn" {
  value = aws_batch_job_queue.aurora.arn
}

output "aurora_job_queue_fast_arn" {
  value = aws_batch_job_queue.aurora_fast.arn
}

output "data_lake_job_definition_arn" {
  value = aws_batch_job_definition.data_lake.arn
}

output "data_lake_job_definition" {
  value = aws_batch_job_definition.data_lake
}

output "data_lake_job_queue_arn" {
  value = aws_batch_job_queue.data_lake.arn
}

output "tile_cache_job_definition_arn" {
  value = aws_batch_job_definition.tile_cache.arn
}

output "tile_cache_job_definition" {
  value = aws_batch_job_definition.tile_cache
}

output "tile_cache_job_queue_arn" {
  value = aws_batch_job_queue.tile_cache.arn
}