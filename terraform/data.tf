# import core state
data "terraform_remote_state" "core" {
  backend = "s3"
  config = {
    bucket = local.tf_state_bucket
    region = "us-east-1"
    key    = "core.tfstate"
  }
}

data "aws_ssm_parameter" "raster_analysis_lambda_contract" {
  name = "/infra/${var.environment}/gfw-raster-analysis-lambda/contract"
}

locals {
  raster_analysis_lambda = jsondecode(data.aws_ssm_parameter.raster_analysis_lambda_contract.value)
}

# import tile_cache state
# This might cause a chicken/ egg problem on new deployments.
# B/C tile cache state also imports data-api state
# In our case, we only need the S3 bucket name here which already exists in all environments.
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
    name_suffix       = replace(local.name_suffix, "-", "_")

    data_lake_bucket         = data.terraform_remote_state.core.outputs.data-lake_bucket
    tile_cache_bucket        = data.terraform_remote_state.tile_cache.outputs.tile_cache_bucket_name
    tile_cache_cloudfront_id = data.terraform_remote_state.tile_cache.outputs.cloudfront_distribution_id
    tile_cache_url           = data.terraform_remote_state.tile_cache.outputs.tile_cache_url
    tile_cache_cluster       = data.terraform_remote_state.tile_cache.outputs.tile_cache_cluster
    tile_cache_service       = data.terraform_remote_state.tile_cache.outputs.tile_cache_service

    aurora_job_definition       = module.batch_job_queues.aurora_job_definition_arn
    aurora_job_queue            = module.batch_job_queues.aurora_job_queue_arn
    aurora_job_queue_fast       = module.batch_job_queues.aurora_job_queue_fast_arn
    data_lake_job_definition    = module.batch_job_queues.data_lake_job_definition_arn
    data_lake_job_queue         = module.batch_job_queues.data_lake_job_queue_arn
    tile_cache_job_definition   = module.batch_job_queues.tile_cache_job_definition_arn
    tile_cache_job_queue        = module.batch_job_queues.tile_cache_job_queue_arn
    pixetl_job_definition       = module.batch_job_queues.pixetl_job_definition_arn
    pixetl_job_queue            = module.batch_job_queues.pixetl_job_queue_arn
    on_demand_compute_job_queue = module.batch_job_queues.on_demand_compute_job_queue_arn
    raster_analysis_lambda_name = "raster-analysis-tiled_raster_analysis-default"
    raster_analysis_sfn_arn     = local.raster_analysis_lambda.raster_analysis_state_machine_arn
    service_url                 = local.service_url
    rw_api_url                  = var.rw_api_url
    rw_api_key_arn              = var.rw_api_key_arn
    api_token_secret_arn        = data.terraform_remote_state.core.outputs.secrets_read-gfw-api-token_arn
    aws_gcs_key_secret_arn      = data.terraform_remote_state.core.outputs.secrets_read-gfw-gee-export_arn

    api_gateway_id                  = var.api_gateway_id == "" ? module.api_gateway[0].api_gateway_id : var.api_gateway_id
    api_gateway_external_usage_plan = var.api_gw_external_app_id == "" ? module.api_gateway[0].external_usage_plan_id : var.api_gw_external_app_id
    api_gateway_internal_usage_plan = var.api_gw_internal_app_id == "" ? module.api_gateway[0].internal_usage_plan_id : var.api_gw_internal_app_id
    api_gateway_stage_name          = var.api_gateway_stage_name
    internal_domains                = var.internal_domains

    # TODO move to core-infrastructure when operational
    new_relic_license_key_arn = var.new_relic_license_key_arn
  }
  depends_on = [
    module.batch_job_queues.aurora_job_definition,
    module.batch_job_queues.data_lake_job_definition,
    module.batch_job_queues.tile_cache_job_definition,
    module.batch_job_queues.pixetl_job_definition
  ]
}

data "template_file" "task_batch_policy" {
  template = file("${path.root}/templates/run_batch_policy.json.tmpl")
  vars = {
    aurora_job_definition_arn       = module.batch_job_queues.aurora_job_definition_arn
    aurora_job_queue_arn            = module.batch_job_queues.aurora_job_queue_arn
    aurora_job_queue_fast_arn       = module.batch_job_queues.aurora_job_queue_fast_arn
    data_lake_job_definition_arn    = module.batch_job_queues.data_lake_job_definition_arn
    data_lake_job_queue_arn         = module.batch_job_queues.data_lake_job_queue_arn
    tile_cache_job_definition_arn   = module.batch_job_queues.tile_cache_job_definition_arn
    tile_cache_job_queue_arn        = module.batch_job_queues.tile_cache_job_queue_arn
    pixetl_job_definition_arn       = module.batch_job_queues.pixetl_job_definition_arn
    pixetl_job_queue_arn            = module.batch_job_queues.pixetl_job_queue_arn
    on_demand_compute_job_queue_arn = module.batch_job_queues.on_demand_compute_job_queue_arn
  }
  depends_on = [
    module.batch_job_queues.aurora_job_definition,
    module.batch_job_queues.data_lake_job_definition,
    module.batch_job_queues.tile_cache_job_definition,
    module.batch_job_queues.pixetl_job_definition
  ]
}

data "template_file" "query_batch_task_policy" {
  template = file("${path.root}/templates/query_batch_policy.json.tmpl")
}

data "local_file" "iam_s3_read_only" {
  filename = "${path.root}/templates/iam_s3_read_only.json"
}

//data "template_file" "iam_lambda_invoke" {
//  template = "${path.root}/templates/lambda_invoke_policy.json.tmpl"
//  vars = {
//    lambda_arn = local.raster_analysis_lambda.raster_analysis_state_machine_arn
//  }
//}

data "local_file" "iam_lambda_invoke" {
  filename = "${path.root}/templates/lambda_invoke_policy.json.tmpl"
}

data "local_file" "iam_api_gateway_policy" {
  filename = "${path.root}/templates/api_gateway_policy.json.tmpl"
}

data "local_file" "cloudwatch_log_policy" {
  filename = "${path.root}/templates/cloudwatch_log_policy.json.tmpl"
}

data "template_file" "lambda_role_policy" {
  template = file("${path.root}/templates/role-trust-policy.json.tmpl")

  vars = {
    service = "lambda"
  }
}

data "template_file" "api_gateway_role_policy" {
  template = file("${path.root}/templates/role-trust-policy.json.tmpl")

  vars = {
    service = "apigateway"
  }
}

data "aws_iam_policy_document" "read_gcs_secret_doc" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [data.terraform_remote_state.core.outputs.secrets_read-gfw-gee-export_arn]
    effect    = "Allow"
  }
}

data "aws_iam_policy_document" "read_new_relic_lic" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.new_relic_license_key_arn]
    effect    = "Allow"
  }
}

data "aws_iam_policy_document" "read_rw_api_key" {
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.rw_api_key_arn]
    effect    = "Allow"
  }
}

data "external" "generate_port" {
  count   = var.environment == "dev" ? 1 : 0
  program = ["python3", "${path.module}/generate_port.py", local.name_suffix, "30000", "31000"]
}

data "template_file" "tile_cache_bucket_policy" {
  template = file("${path.root}/templates/tile_cache_bucket_policy.json.tmpl")

  vars = {
    bucket_arn = data.terraform_remote_state.tile_cache.outputs.tile_cache_bucket_arn
  }
}

data "template_file" "step_function_policy" {
  template = file("${path.root}/templates/step_function_policy.json.tmpl")
  vars = {
    raster_analysis_state_machine_arn = local.raster_analysis_lambda.raster_analysis_state_machine_arn
  }
}

# Hash of the contents of the FastAPI app docker. The docker commands run in the main
# directory (parent directory of terraform directory), and the Docker file is in the
# same directory.
data "external" "hash" {
  program = ["${path.root}/scripts/hash.sh", "${path.root}/../", "."]
}
