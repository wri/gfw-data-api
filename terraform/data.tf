# import core state
data "terraform_remote_state" "core" {
  backend = "s3"
  config = {
    bucket = local.tf_state_bucket
    region = "us-east-1"
    key    = "core.tfstate"
  }
}


# import pixetl state
data "terraform_remote_state" "pixetl" {
  backend = "s3"
  config = {
    bucket = local.tf_state_bucket
    region = "us-east-1"
    key    = "wri__gfw_pixetl.tfstate"
  }
}

data "template_file" "container_definition" {
  template = file("${path.root}/templates/container_definition.json.tmpl")
  vars = {
    image = "${module.app_docker_image.repository_url}:latest"

    container_name = var.container_name
    container_port = var.container_port

    log_group = aws_cloudwatch_log_group.default.name

    reader_secret_arn = data.terraform_remote_state.core.outputs.secrets_postgresql-reader_arn
    writer_secret_arn = data.terraform_remote_state.core.outputs.secrets_postgresql-writer_arn
    log_level         = var.log_level
    project           = local.project
    environment       = var.environment
    aws_region        = var.region

    aurora_job_definition     = module.batch_job_queues.aurora_job_definition
    aurora_job_queue          = module.batch_job_queues.aurora_job_queue
    data_lake_job_definition  = module.batch_job_queues.data_lake_job_definition
    data_lake_job_queue       = module.batch_job_queues.data_lake_job_queue
    tile_cache_job_definition = module.batch_job_queues.tile_cache_job_definition
    tile_cache_job_queue      = module.batch_job_queues.tile_cache_job_queue
    pixetl_job_definition     = data.terraform_remote_state.pixetl.outputs.job_definition_arn
    pixetl_job_queue          = data.terraform_remote_state.pixetl.outputs.job_queue_arn

  }
}

data "template_file" "task_batch_policy" {
  template = file("${path.root}/templates/batch_policy.json.tmpl")
  vars = {
    aurora_compute_environment_arn     = module.batch_aurora_writer.arn
    aurora_job_definition_arn          = module.batch_job_queues.aurora_job_definition
    aurora_job_queue_arn               = module.batch_job_queues.aurora_job_queue
    data_lake_compute_environment_arn  = module.batch_data_lake_writer.arn
    data_lake_job_definition_arn       = module.batch_job_queues.data_lake_job_definition
    data_lake_job_queue_arn            = module.batch_job_queues.data_lake_job_queue
    tile_cache_compute_environment_arn = module.batch_data_lake_writer.arn
    tile_cache_job_definition_arn      = module.batch_job_queues.tile_cache_job_definition
    tile_cache_job_queue_arn           = module.batch_job_queues.tile_cache_job_queue
    pixetl_compute_environment_arn     = data.terraform_remote_state.pixetl.outputs.compute_environment_arn
    pixetl_job_definition_arn          = data.terraform_remote_state.pixetl.outputs.job_definition_arn
    pixetl_job_queue_arn               = data.terraform_remote_state.pixetl.outputs.job_queue_arn
  }
}