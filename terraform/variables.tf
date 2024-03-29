variable "environment" {
  type        = string
  description = "An environment namespace for the infrastructure."
}

variable "region" {
  default = "us-east-1"
  type    = string
}

variable "container_name" {
  default = "gfw-data-api"
  type    = string
}
variable "container_port" {
  default = 80
  type    = number
}
variable "log_level" {
  type = string
}
variable "log_retention" {
  type    = number
  default = 30
}
variable "desired_count" {
  type = number
}
variable "fargate_cpu" {
  type    = number
  default = 512
}
variable "fargate_memory" {
  type    = number
  default = 2048
}
variable "auto_scaling_cooldown" {
  type    = number
  default = 300
}
variable "auto_scaling_max_capacity" {
  type = number
}
variable "auto_scaling_max_cpu_util" {
  type    = number
  default = 75
}
variable "auto_scaling_min_capacity" {
  type = number
}
variable "key_pair" {
  type = string
}

variable "service_url" {
  type = string
}

variable "rw_api_url" {
  type = string
}

variable "git_sha" {
  type = string
}

variable "lambda_analysis_workspace" {
  type = string
}

variable "data_lake_max_vcpus" {
  type    = number
  default = 576
}

variable "api_gateway_usage_plans" {
  type        = map(any)
  description = "Throttling limits for API Gateway"
  default = {
    internal_apps = {
      quota_limit = 10000 # per day
      burst_limit = 100   # per second
      rate_limit  = 200
    }
    external_apps = {
      quota_limit = 500
      burst_limit = 10
      rate_limit  = 20
    }
  }
}

variable "internal_domains" {
  type        = string
  description = "Comma separated list of client domains for which we set first tier rate limiting."
  default     = "*.globalforestwatch.org,globalforestwatch.org,api.resourcewatch.org,my.gfw-mapbuilder.org,resourcewatch.org"
}

variable "download_endpoints" {
  type        = list(string)
  description = "path parts to download endpoints"

  # listing spatial endpoints as gateway needs them explicitly created
  # in order to apply endpoint-level throttling to them
  default = ["geotiff", "gpkg", "shp"]
}

#TODO import from core-infrastructure when operational
variable "new_relic_license_key_arn" {
  type        = string
  description = "New Relic license key ARN"
}
