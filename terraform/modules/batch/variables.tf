variable "project" { type = string }
variable "name_suffix" { type = string }
variable "aurora_compute_environment_arn" { type = string }
variable "data_lake_compute_environment_arn" { type = string }
variable "tile_cache_compute_environment_arn" { type = string }
variable "pixetl_compute_environment_arn" { type = string }
variable "gdal_repository_url" { type = string }
variable "postgres_repository_url" { type = string }
variable "pixetl_repository_url" { type = string }
variable "tile_cache_repository_url" { type = string }
variable "environment" { type = string }
variable "s3_write_data-lake_arn" { type = string }
variable "s3_write_tile-cache_arn" { type = string }
variable "reader_secret_arn" { type = string }
variable "writer_secret_arn" { type = string }
variable "aurora_max_vcpus" { type = number }
variable "gcs_secret" { type = string }
