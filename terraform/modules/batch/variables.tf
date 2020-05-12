variable "project" { type = string }
variable "name_suffix" { type = string }
variable "aurora_compute_environment_arn" { type = string }
variable "data_lake_compute_environment_arn" { type = string }
variable "tile_cache_compute_environment_arn" { type = string }
variable "repository_url" { type = string }
variable "environment" { type = string }
variable "s3_write_data-lake_arn" { type = string }