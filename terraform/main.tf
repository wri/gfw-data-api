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
  bucket_suffix   = var.environment == "production" ? "" : "-${var.environment}"
  tf_state_bucket = "gfw-terraform${local.bucket_suffix}"
  tags            = data.terraform_remote_state.core.outputs.tags
  batch_tags = merge(
    {
      Job = "Data-API Batch Job",
  }, local.tags)
  fargate_tags = merge(
    {
      Job = "Data-API Service",
  }, local.tags)
  name_suffix           = terraform.workspace == "default" ? "" : "-${terraform.workspace}"
  project               = "gfw-data-api"
  aurora_instance_class = data.terraform_remote_state.core.outputs.aurora_cluster_instance_class
  aurora_max_vcpus      = local.aurora_instance_class == "db.t3.medium" ? 2 : local.aurora_instance_class == "db.r6g.large" ? 2 : local.aurora_instance_class == "db.r6g.xlarge" ? 4 : local.aurora_instance_class == "db.r6g.2xlarge" ? 8 : local.aurora_instance_class == "db.r6g.4xlarge" ? 16 : local.aurora_instance_class == "db.r6g.8xlarge" ? 32 : local.aurora_instance_class == "db.r6g.16xlarge" ? 64 : local.aurora_instance_class == "db.r5.large" ? 2 : local.aurora_instance_class == "db.r5.xlarge" ? 4 : local.aurora_instance_class == "db.r5.2xlarge" ? 8 : local.aurora_instance_class == "db.r5.4xlarge" ? 16 : local.aurora_instance_class == "db.r5.8xlarge" ? 32 : local.aurora_instance_class == "db.r5.12xlarge" ? 48 : local.aurora_instance_class == "db.r5.16xlarge" ? 64 : local.aurora_instance_class == "db.r5.24xlarge" ? 96 : ""
  service_url           = var.environment == "dev" ? "http://${module.fargate_autoscaling.lb_dns_name}" : var.service_url
  container_tag         = substr(var.git_sha, 0, 7)
}


# Docker image for FastAPI app
module "app_docker_image" {
  source     = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2"
  image_name = lower("${local.project}${local.name_suffix}")
  root_dir   = "${path.root}/../"
  tag        = local.container_tag
}


# Docker image for GDAL Python Batch jobs
module "batch_gdal_python_image" {
  source          = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2"
  image_name      = lower("${local.project}-gdal_python${local.name_suffix}")
  root_dir        = "${path.root}/../"
  docker_path     = "batch"
  docker_filename = "gdal-python.dockerfile"
}

# Docker image for PixETL Batch jobs
module "batch_pixetl_image" {
  source          = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2"
  image_name      = lower("${local.project}-pixetl${local.name_suffix}")
  root_dir        = "${path.root}/../"
  docker_path     = "batch"
  docker_filename = "pixetl.dockerfile"
}

# Docker image for PostgreSQL Client Batch jobs
module "batch_postgresql_client_image" {
  source          = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2"
  image_name      = lower("${local.project}-postgresql_client${local.name_suffix}")
  root_dir        = "${path.root}/../"
  docker_path     = "batch"
  docker_filename = "postgresql-client.dockerfile"
}

# Docker image for Tile Cache Batch jobs
module "batch_tile_cache_image" {
  source          = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2"
  image_name      = lower("${local.project}-tile_cache${local.name_suffix}")
  root_dir        = "${path.root}/../"
  docker_path     = "batch"
  docker_filename = "tile_cache.dockerfile"
}


module "fargate_autoscaling" {
  source                    = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/fargate_autoscaling?ref=v0.4.2"
  project                   = local.project
  name_suffix               = local.name_suffix
  tags                      = local.fargate_tags
  vpc_id                    = data.terraform_remote_state.core.outputs.vpc_id
  private_subnet_ids        = data.terraform_remote_state.core.outputs.private_subnet_ids
  public_subnet_ids         = data.terraform_remote_state.core.outputs.public_subnet_ids
  container_name            = var.container_name
  container_port            = var.container_port
  desired_count             = var.desired_count
  fargate_cpu               = var.fargate_cpu
  fargate_memory            = var.fargate_memory
  auto_scaling_cooldown     = var.auto_scaling_cooldown
  auto_scaling_max_capacity = var.auto_scaling_max_capacity
  auto_scaling_max_cpu_util = var.auto_scaling_max_cpu_util
  auto_scaling_min_capacity = var.auto_scaling_min_capacity
//  acm_certificate_arn       = var.environment == "dev" ? null : data.terraform_remote_state.core.outputs.acm_certificate
  security_group_ids        = [data.terraform_remote_state.core.outputs.postgresql_security_group_id]
  task_role_policies = [
    data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn,
    aws_iam_policy.run_batch_jobs.arn,
    aws_iam_policy.s3_read_only.arn,
    aws_iam_policy.lambda_invoke.arn,
    data.terraform_remote_state.tile_cache.outputs.ecs_update_service_policy_arn,
    data.terraform_remote_state.tile_cache.outputs.tile_cache_bucket_full_access_policy_arn,
    data.terraform_remote_state.tile_cache.outputs.cloudfront_invalidation_policy_arn
  ]
  task_execution_role_policies = [
    aws_iam_policy.query_batch_jobs.arn,
    data.terraform_remote_state.core.outputs.secrets_postgresql-reader_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_postgresql-writer_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_read-gfw-api-token_policy_arn
  ]
  container_definition = data.template_file.container_definition.rendered
}

# Using instance types with 1 core only
module "batch_aurora_writer" {
  source = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/compute_environment?ref=v0.4.2"
  ecs_role_policy_arns = [
    data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn,
    data.terraform_remote_state.core.outputs.secrets_postgresql-reader_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_postgresql-writer_policy_arn,
    aws_iam_policy.query_batch_jobs.arn,
    aws_iam_policy.s3_read_only.arn
  ]
  instance_types = ["c5.large", "c4.large", "m5.large", "m4.large"]
  # "a1.medium" works but needs special ARM docker file
  # currently not supported but want to have "m6g.medium", "t2.nano", "t2.micro", "t2.small"
  key_pair  = var.key_pair
  max_vcpus = local.aurora_max_vcpus
  project   = local.project
  security_group_ids = [
    data.terraform_remote_state.core.outputs.default_security_group_id,
    data.terraform_remote_state.core.outputs.postgresql_security_group_id
  ]
  subnets                  = data.terraform_remote_state.core.outputs.private_subnet_ids
  suffix                   = local.name_suffix
  tags                     = local.batch_tags
  use_ephemeral_storage    = false
  ebs_volume_size          = 32
  compute_environment_name = "aurora_sql_writer"
}


module "batch_data_lake_writer" {
  source = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/compute_environment?ref=v0.4.2"
  ecs_role_policy_arns = [
    aws_iam_policy.query_batch_jobs.arn,
    aws_iam_policy.s3_read_only.arn,
    data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn,
    data.terraform_remote_state.tile_cache.outputs.tile_cache_bucket_write_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_postgresql-reader_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_postgresql-writer_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_read-gfw-gee-export_policy_arn
  ]
  key_pair  = var.key_pair
  max_vcpus = var.data_lake_max_vcpus
  project   = local.project
  security_group_ids = [
    data.terraform_remote_state.core.outputs.default_security_group_id,
    data.terraform_remote_state.core.outputs.postgresql_security_group_id
  ]
  subnets                  = data.terraform_remote_state.core.outputs.private_subnet_ids
  suffix                   = local.name_suffix
  tags                     = local.batch_tags
  use_ephemeral_storage    = true
  compute_environment_name = "data_lake_writer"
}

module "batch_job_queues" {
  source                             = "./modules/batch"
  aurora_compute_environment_arn     = module.batch_aurora_writer.arn
  data_lake_compute_environment_arn  = module.batch_data_lake_writer.arn
  pixetl_compute_environment_arn     = module.batch_data_lake_writer.arn
  tile_cache_compute_environment_arn = module.batch_data_lake_writer.arn
  environment                        = var.environment
  name_suffix                        = local.name_suffix
  project                            = local.project
  gdal_repository_url                = "${module.batch_gdal_python_image.repository_url}:latest"
  pixetl_repository_url              = "${module.batch_pixetl_image.repository_url}:latest"
  postgres_repository_url            = "${module.batch_postgresql_client_image.repository_url}:latest"
  tile_cache_repository_url          = "${module.batch_tile_cache_image.repository_url}:latest"
  iam_policy_arn = [
    "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    aws_iam_policy.query_batch_jobs.arn,
    data.terraform_remote_state.core.outputs.iam_policy_s3_write_data-lake_arn,
    data.terraform_remote_state.tile_cache.outputs.tile_cache_bucket_write_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_postgresql-reader_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_postgresql-writer_policy_arn,
    data.terraform_remote_state.core.outputs.secrets_read-gfw-gee-export_policy_arn
  ]
  aurora_max_vcpus = local.aurora_max_vcpus
  gcs_secret       = data.terraform_remote_state.core.outputs.secrets_read-gfw-gee-export_arn
}