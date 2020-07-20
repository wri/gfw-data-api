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


# import tile_cache state
# This might cause a chicken/ egg problem on new deployments.
# B/C tile cache state also imports data-api state
# In our case, we only need the S3 bucket name here which already exists in all envionments.
# If we were to migrate this project to a different account, you would need to create bucket manually first
# and import into tile cache state for this to work
data "terraform_remote_state" "tile_cache" {
  backend = "s3"
  config = {
    bucket = local.tf_state_bucket
    region = "us-east-1"
    key    = "wri__gfw_fire-vector-tiles.tfstate"
  }
}

data "template_file" "container_definition" {
  template = file("${path.root}/templates/container_definition.json.tmpl")
  vars = {
    image = "${module.app_docker_image.repository_url}:${local.container_tag}"

    container_name = var.container_name
    container_port = var.container_port

    log_group = aws_cloudwatch_log_group.default.name

    reader_secret_arn = data.terraform_remote_state.core.outputs.secrets_postgresql-reader_arn
    writer_secret_arn = data.terraform_remote_state.core.outputs.secrets_postgresql-writer_arn
    log_level         = var.log_level
    project           = local.project
    environment       = var.environment
    aws_region        = var.region

    data_lake_bucket         = data.terraform_remote_state.core.outputs.data-lake_bucket
    tile_cache_bucket        = data.terraform_remote_state.tile_cache.outputs.tile_cache_bucket_name
    tile_cache_cloudfront_id = data.terraform_remote_state.tile_cache.outputs.cloudfront_distribution_id
    tile_cache_url           = data.terraform_remote_state.tile_cache.outputs.tile_cache_url
    tile_cache_cluster       = data.terraform_remote_state.tile_cache.outputs.tile_cache_cluster
    tile_cache_service       = data.terraform_remote_state.tile_cache.outputs.tile_cache_service

    aurora_job_definition     = module.batch_job_queues.aurora_job_definition
    aurora_job_queue          = module.batch_job_queues.aurora_job_queue
    data_lake_job_definition  = module.batch_job_queues.data_lake_job_definition
    data_lake_job_queue       = module.batch_job_queues.data_lake_job_queue
    tile_cache_job_definition = module.batch_job_queues.tile_cache_job_definition
    tile_cache_job_queue      = module.batch_job_queues.tile_cache_job_queue
    pixetl_job_definition     = data.terraform_remote_state.pixetl.outputs.job_definition_arn
    pixetl_job_queue          = data.terraform_remote_state.pixetl.outputs.job_queue_arn

    service_url          = local.service_url
    api_token_secret_arn = data.terraform_remote_state.core.outputs.secrets_read-gfw-api-token_arn

  }
}

locals {

  # trying very inelegantly to replace the batch job definition revision number with a wildcard,
  # so that the task can access any

  aurora_job_definition_arn_list =  split(":",module.batch_job_queues.aurora_job_definition)
  aurora_job_definition_arn_list_short = slice(arn_list, 0, length(local.aurora_job_definition_arn_list)-2)
  aurora_job_definition_new_arn  = join(":", concat(local.aurora_job_definition_arn_list_short + ["*"]))

  data_lake_job_definition_arn_list =  split(":",module.batch_job_queues.data_lake_job_definition)
  data_lake_job_definition_arn_list_short = slice(arn_list, 0, length(local.data_lake_job_definition_arn_list)-2)
  data_lake_job_definition_new_arn  = join(":", concat(local.data_lake_job_definition_arn_list_short + ["*"]))

  tile_cache_job_definition_arn_list =  split(":",module.batch_job_queues.tile_cache_job_definition)
  tile_cache_job_definition_arn_list_short = slice(arn_list, 0, length(local.tile_cache_job_definition_arn_list)-2)
  tile_cache_job_definition_new_arn  = join(":", concat(local.tile_cache_job_definition_arn_list_short + ["*"]))

  pixetl_job_definition_arn_list =  split(":",data.terraform_remote_state.pixetl.outputs.job_definition_arn)
  pixetl_job_definition_arn_list_short = slice(arn_list, 0, length(local.pixetl_job_definition_arn_list)-2)
  pixetl_job_definition_new_arn  = join(":", concat(local.pixetl_job_definition_arn_list_short + ["*"]))
}

data "template_file" "task_batch_policy" {
  template = file("${path.root}/templates/batch_policy.json.tmpl")
  vars = {
    aurora_job_definition_arn     = local.aurora_job_definition_new_arn
    aurora_job_queue_arn          = module.batch_job_queues.aurora_job_queue
    data_lake_job_definition_arn  = local.data_lake_job_definition_new_arn
    data_lake_job_queue_arn       = module.batch_job_queues.data_lake_job_queue
    tile_cache_job_definition_arn = local.tile_cache_job_definition_new_arn
    tile_cache_job_queue_arn      = module.batch_job_queues.tile_cache_job_queue
    pixetl_job_definition_arn     = local.pixetl_job_definition_new_arn
    pixetl_job_queue_arn          = data.terraform_remote_state.pixetl.outputs.job_queue_arn
  }
}

data "local_file" "iam_s3_read_only" {
  filename = "${path.root}/templates/iam_s3_read_only.json"
}