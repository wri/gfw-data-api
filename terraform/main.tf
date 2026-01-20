# Require TF version to be same as or greater than 0.12.24
terraform {
  backend "s3" {
    region  = "us-east-1"
    key     = "wri__gfw-data-api.tfstate"
    encrypt = true
  }
}


# some local
locals {
  tags            = local.core.tags
  fargate_tags = merge(
    {
      Job = "Data-API Service",
  }, local.tags)
  name_suffix           = terraform.workspace == "default" ? "" : "-${terraform.workspace}"
  project               = "gfw-data-api"
  aurora_instance_class = local.core.aurora_cluster_instance_class
  aurora_max_vcpus      = local.aurora_instance_class == "db.t3.medium" ? 2 : local.aurora_instance_class == "db.r6g.large" ? 2 : local.aurora_instance_class == "db.r6g.xlarge" ? 4 : local.aurora_instance_class == "db.r6g.2xlarge" ? 8 : local.aurora_instance_class == "db.r6g.4xlarge" ? 16 : local.aurora_instance_class == "db.r6g.8xlarge" ? 32 : local.aurora_instance_class == "db.r6g.16xlarge" ? 64 : local.aurora_instance_class == "db.r5.large" ? 2 : local.aurora_instance_class == "db.r5.xlarge" ? 4 : local.aurora_instance_class == "db.r5.2xlarge" ? 8 : local.aurora_instance_class == "db.r5.4xlarge" ? 16 : local.aurora_instance_class == "db.r5.8xlarge" ? 32 : local.aurora_instance_class == "db.r5.12xlarge" ? 48 : local.aurora_instance_class == "db.r5.16xlarge" ? 64 : local.aurora_instance_class == "db.r5.24xlarge" ? 96 : ""
  service_url           = var.environment == "dev" ? "http://${local.lb_dns_name}:${data.external.generate_port[0].result["port"]}" : var.service_url
  # The container_registry module only pushes a new Docker image if the docker hash
  # computed by its hash.sh script has changed. So, we make the container tag exactly
  # be that hash. Therefore, we will know that either the previous docker with the
  # same contents and tag will already exist, if nothing has changed in the docker
  # image, or the container registry module will push a new docker with the tag we
  # want.
  container_tag = lookup(data.external.hash.result, "hash")
  lb_dns_name   = coalesce(module.fargate_autoscaling.lb_dns_name, var.lb_dns_name)
}

# Docker image for FastAPI app
module "app_docker_image" {
  source       = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2.9"
  image_name   = substr(lower("${local.project}${local.name_suffix}"), 0, 64)
  root_dir     = "${path.root}/../"
  tag          = local.container_tag
  force_delete = var.force_delete_ecr_repos
}

# Docker image for PixETL Batch jobs
module "batch_pixetl_image" {
  source          = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2.9"
  image_name      = substr(lower("${local.project}-pixetl${local.name_suffix}"), 0, 64)
  root_dir        = "${path.root}/../"
  docker_path     = "batch"
  docker_filename = "pixetl.dockerfile"
  force_delete    = var.force_delete_ecr_repos
}

# Docker image for all Batch jobs except those requiring PixETL
module "batch_universal_image" {
  source          = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2.9"
  image_name      = substr(lower("${local.project}-universal${local.name_suffix}"), 0, 64)
  root_dir        = "${path.root}/../"
  docker_path     = "batch"
  docker_filename = "universal_batch.dockerfile"
  # Only force delete ECR repos in dev, just in case
  force_delete = var.force_delete_ecr_repos
}

module "fargate_autoscaling" {
  source                       = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/fargate_autoscaling?ref=v0.4.2.5"
  project                      = local.project
  name_suffix                  = local.name_suffix
  tags                         = local.fargate_tags
  vpc_id                       = local.core.vpc_id
  private_subnet_ids           = local.core.private_subnet_ids
  public_subnet_ids            = local.core.public_subnet_ids
  container_name               = var.container_name
  container_port               = var.container_port
  desired_count                = var.desired_count
  fargate_cpu                  = var.fargate_cpu
  fargate_memory               = var.fargate_memory
  load_balancer_arn            = var.load_balancer_arn
  load_balancer_security_group = var.load_balancer_security_group
  listener_port                = var.environment == "dev" ? data.external.generate_port[0].result.port : var.listener_port
  auto_scaling_cooldown        = var.auto_scaling_cooldown
  auto_scaling_max_capacity    = var.auto_scaling_max_capacity
  auto_scaling_max_cpu_util    = var.auto_scaling_max_cpu_util
  auto_scaling_min_capacity    = var.auto_scaling_min_capacity
  // acm_certificate_arn       = var.environment == "dev" ? null : local.core.acm_certificate_arn
  security_group_ids = [local.core.postgresql_security_group_id]
  task_role_policies = [
    local.core.iam_policy_s3_write_data_lake_arn,
    aws_iam_policy.run_batch_jobs.arn,
    aws_iam_policy.s3_read_only.arn,
    aws_iam_policy.lambda_invoke.arn,
    aws_iam_policy.iam_api_gateway_policy.arn,
    aws_iam_policy.read_gcs_secret.arn,
    local.tile_cache.ecs_update_service_policy_arn,
    aws_iam_policy.tile_cache_bucket_policy.arn,
    local.tile_cache.cloudfront_invalidation_policy_arn,
    aws_iam_policy.step_function_policy.arn,
  ]
  task_execution_role_policies = [
    aws_iam_policy.query_batch_jobs.arn,
    aws_iam_policy.read_new_relic_secret.arn,
    aws_iam_policy.read_rw_api_key_secret.arn,
    local.core.postgresql_reader_policy_arn,
    local.core.postgresql_writer_policy_arn,
    local.core.gfw_data_api_token_read_policy_arn
  ]
  container_definition = data.template_file.container_definition.rendered
}

# Create compute environment for DB writer
# Using instance types with 1 core only, and EC2 instances (not SPOT).
module "batch_aurora_writer" {
  source = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/compute_environment?ref=v0.4.2.5"
  ecs_role_policy_arns = [
    local.core.iam_policy_s3_write_data_lake_arn,
    local.core.postgresql_reader_policy_arn,
    local.core.postgresql_writer_policy_arn,
    aws_iam_policy.query_batch_jobs.arn,
    aws_iam_policy.s3_read_only.arn
  ]
  instance_types = [
    "c6a.large", "c6i.large", "c5a.large", "c5.large", "c4.large",
    "m6a.large", "m6i.large", "m5a.large", "m5.large", "m4.large"
  ]
  # "a1.medium" works but needs special ARM docker file
  # currently not supported but want to have "m6g.medium", "t2.nano", "t2.micro", "t2.small"
  key_pair  = var.key_pair
  max_vcpus = local.aurora_max_vcpus
  project   = local.project
  security_group_ids = [
    local.core.default_security_group_id,
    local.core.postgresql_security_group_id
  ]
  subnets                  = local.core.private_subnet_ids
  suffix                   = local.name_suffix
  tags                     = merge(local.tags, {Job = "Aurora Writer",})
  use_ephemeral_storage    = false
  ebs_volume_size          = 60
  compute_environment_name = "aurora_sql_writer"
  launch_type              = "EC2"
}


# Create compute environment for data lake writing, pixetl, and tile cache jobs
# Currently does EC2 instances, not spot instances.
module "batch_data_lake_writer" {
  source = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/compute_environment?ref=v0.4.2.5"
  ecs_role_policy_arns = [
    aws_iam_policy.query_batch_jobs.arn,
    aws_iam_policy.s3_read_only.arn,
    local.core.iam_policy_s3_write_data_lake_arn,
    local.tile_cache.tile_cache_bucket_write_policy_arn,
    local.core.postgresql_reader_policy_arn,
    local.core.postgresql_writer_policy_arn,
    local.core.gfw_gee_export_read_policy_arn
  ]
  key_pair  = var.key_pair
  max_vcpus = var.data_lake_max_vcpus
  project   = local.project
  security_group_ids = [
    local.core.default_security_group_id,
    local.core.postgresql_security_group_id
  ]
  subnets               = local.core.private_subnet_ids
  suffix                = local.name_suffix
  tags                  = merge(local.tags, {Job = "Datalake/pixetl/tile-cache",} )
  use_ephemeral_storage = true
  launch_type              = "EC2"
  instance_types           = var.data_lake_writer_instance_types
  compute_environment_name = "data_lake_writer"
}

# Creating compute environment for cogify jobs
# Should always use EC2 instances, since jobs run for so long.
module "batch_cogify" {
  source = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/compute_environment?ref=v0.4.2.5"
  ecs_role_policy_arns = [
    aws_iam_policy.query_batch_jobs.arn,
    aws_iam_policy.s3_read_only.arn,
    local.core.iam_policy_s3_write_data_lake_arn,
    local.core.postgresql_reader_policy_arn,
    local.core.postgresql_writer_policy_arn,
    local.core.gfw_gee_export_read_policy_arn
  ]
  key_pair  = var.key_pair
  max_vcpus = var.data_lake_max_vcpus
  project   = local.project
  security_group_ids = [
    local.core.default_security_group_id,
    local.core.postgresql_security_group_id
  ]
  subnets                  = local.core.private_subnet_ids
  suffix                   = local.name_suffix
  tags                     = merge(local.tags, {Job = "COGify",}, )
  use_ephemeral_storage    = true
  launch_type              = "EC2"
  instance_types           = var.data_lake_writer_instance_types
  compute_environment_name = "batch_cogify"
}

# Create aurora, aurora_fast, data_lake, pixetl, tile cache, and ondemand job queues.
module "batch_job_queues" {
  source                             = "./modules/batch"
  aurora_compute_environment_arn     = module.batch_aurora_writer.arn
  data_lake_compute_environment_arn  = module.batch_data_lake_writer.arn
  pixetl_compute_environment_arn     = module.batch_data_lake_writer.arn
  tile_cache_compute_environment_arn = module.batch_data_lake_writer.arn
  cogify_compute_environment_arn     = module.batch_cogify.arn
  environment                        = var.environment
  name_suffix                        = local.name_suffix
  project                            = local.project
  gdal_repository_url                = "${module.batch_universal_image.repository_url}:latest"
  pixetl_repository_url              = "${module.batch_pixetl_image.repository_url}:latest"
  postgres_repository_url            = "${module.batch_universal_image.repository_url}:latest"
  tile_cache_repository_url          = "${module.batch_universal_image.repository_url}:latest"
  iam_policy_arn = [
    "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    aws_iam_policy.query_batch_jobs.arn,
    local.core.iam_policy_s3_write_data_lake_arn,
    local.tile_cache.tile_cache_bucket_write_policy_arn,
    local.core.postgresql_reader_policy_arn,
    local.core.postgresql_writer_policy_arn,
    local.core.gfw_gee_export_read_policy_arn
  ]
  aurora_max_vcpus = local.aurora_max_vcpus
  gcs_secret       = local.core.gfw_gee_export_secret_arn
}

module "api_gateway" {
  count                   = var.api_gateway_id == "" ? 1 : 0
  source                  = "./modules/api_gateway/gateway"
  lb_dns_name             = local.lb_dns_name
  api_gateway_role_policy = data.template_file.api_gateway_role_policy.rendered
  lambda_role_policy      = data.template_file.lambda_role_policy.rendered
  cloudwatch_policy       = data.local_file.cloudwatch_log_policy.content
  lambda_invoke_policy    = data.local_file.iam_lambda_invoke.content
  api_gateway_usage_plans = var.api_gateway_usage_plans
  service_url             = var.service_url
}
