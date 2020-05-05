# Require TF version to be same as or greater than 0.12.24
terraform {
  required_version = ">=0.12.24"
  backend "s3" {
    region  = "us-east-1"
    key     = "wri__gfw-data-api.tfstate"
    encrypt = true
  }
}

# Download any stable version in AWS provider of 2.36.0 or higher in 2.36 train
provider "aws" {
  region  = "us-east-1"
  version = "~> 2.56.0"
}

# some local
locals {
  bucket_suffix   = var.environment == "production" ? "" : "-${var.environment}"
  tf_state_bucket = "gfw-terraform${local.bucket_suffix}"
  tags            = data.terraform_remote_state.core.outputs.tags
  name_suffix     = terraform.workspace == "default" ? "" : "-${terraform.workspace}"
  project         = "gfw-data-api"
}


# Docker file for FastAPI app
module "container_registry" {
  source     = "git::https://github.com/wri/gfw-terraform-modules.git//modules/container_registry?ref=v0.0.7"
  image_name = lower("${local.project}${local.name_suffix}")
  root_dir   = "../${path.root}"
}

module "fargate_autoscaling" {
  source                       = "git::https://github.com/wri/gfw-terraform-modules.git//modules/fargate_autoscaling"
  project                      = local.project
  name_suffix                  = local.name_suffix
  tags                         = local.tags
  vpc_id                       = data.terraform_remote_state.core.outputs.vpc_id
  private_subnet_ids           = data.terraform_remote_state.core.outputs.private_subnet_ids
  public_subnet_ids            = data.terraform_remote_state.core.outputs.public_subnet_ids
  load_balancer_arn            = data.terraform_remote_state.core.outputs.load_balancer_arn
  load_balancer_security_group = data.terraform_remote_state.core.outputs.load_balancer_security_group_id
  container_name               = var.container_name
  container_port               = var.container_port
  listener_port                = data.terraform_remote_state.core.outputs.data_api_listener_port
  desired_count                = 0
  fargate_cpu                  = 256
  fargate_memory               = 2048
  auto_scaling_cooldown        = 300
  auto_scaling_max_capacity    = 15
  auto_scaling_max_cpu_util    = 75
  auto_scaling_min_capacity    = 0
  security_group_ids           = [data.terraform_remote_state.core.outputs.postgresql_security_group_id, data.terraform_remote_state.core.outputs.data_api_load_balancer_security_group_id]
  task_role_policy_arn         = aws_iam_policy.postgresql-secrets_policy.arn
  container_definition         = data.template_file.container_definition.rendered

}


module "postgresql-secrets_policy" {
  source = "git::https://github.com/cloudposse/terraform-aws-iam-policy-document-aggregator.git?ref=0.2.0"
  source_documents = [
    data.terraform_remote_state.core.outputs.secrets_postgresql-reader_policy,
    data.terraform_remote_state.core.outputs.secrets_postgresql-writer_policy
  ]
}

resource "aws_iam_policy" "postgresql-secrets_policy" {
  name   = "${local.project}-postgresql-secrets_policy"
  policy = module.postgresql-secrets_policy.result_document
}