# Require TF version to be same as or greater than 0.12.24
terraform {
  required_version = ">=0.12.24"
  backend "s3" {
    region  = "us-east-1"
    key     = "wri__gfw-terraform.tfstate" # TODO: rename, make sure that prefixes reflect your github repo name
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
  project         = "gfw-terraform" # TODO: rename to your project
}

# import core state
data "terraform_remote_state" "core" {
  backend = "s3"
  config = {
    bucket = local.tf_state_bucket
    region = "us-east-1"
    key    = "core.tfstate"
  }
}