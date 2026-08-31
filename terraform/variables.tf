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

variable "rw_api_key_arn" {
  type        = string
  description = "RW API key ARN"
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

variable "internal_domains" {
  type        = string
  description = "Comma separated list of client domains for which we set first tier rate limiting."
  default     = "*.globalforestwatch.org,globalforestwatch.org,api.resourcewatch.org,my.gfw-mapbuilder.org,resourcewatch.org"
}


#TODO import from core-infrastructure when operational
variable "new_relic_license_key_arn" {
  type        = string
  description = "New Relic license key ARN"
}

variable "load_balancer_arn" {
  type        = string
  default     = ""
  description = "Optional Load Balancer to use for fargate cluster. When left blank, a new LB will be created"
}

variable "load_balancer_security_group" {
  type        = string
  default     = ""
  description = "Optional secuirty group of load balancer with which the task can communicate. Required if load_blancer_arn is not empty"
}

variable "listener_port" {
  type        = number
  description = "The default port the Load Balancer should listen to. Will be ignored when acm_certificate is set."
  default     = 80
}

variable "lb_dns_name" {
  type        = string
  default     = ""
  description = "DNS name of load balancer for API Gateway to forward requests to. API Gateway will first look for one from fargate autoscaling module output before using this."
}

variable "create_cloudfront_distribution" {
  type    = bool
  default = true
}

variable "api_gateway_id" {
  type        = string
  description = "ID of API Gateway instance"
  default     = ""
}

variable "api_gw_internal_app_id" {
  type        = string
  description = "ID of API Gateway usage plan for internal domains"
  default     = ""
}

variable "api_gw_external_app_id" {
  type        = string
  description = "ID of API Gateway usage plan for external domains"
  default     = ""
}

variable "api_gateway_name" {
  type        = string
  description = "Name of API Gateway instance"
  default     = "GFWDataAPIGateway"
}

variable "api_gateway_description" {
  type        = string
  description = "Description of API Gateway Instance"
  default     = "GFW Data API Gateway"
}

variable "api_gateway_stage_name" {
  type        = string
  description = "Deployment stage name of API Gateway instance"
  default     = "deploy"
}

variable "api_gateway_url" {
  type        = string
  description = "The invoke url of the API Gateway stage"
  default     = ""
}

variable "data_lake_writer_instance_types" {
  type        = list(string)
  description = "Graviton (arm64) memory/compute optimized EC2 instances with local NVMe SSDs for data lake writer and cogify batch queues (r7gd/r6gd -- the arm64 counterpart of the r6id/r5ad/r5d families this project used on x86_64)."
  default = [
    "r7gd.large", "r7gd.xlarge", "r7gd.2xlarge", "r7gd.4xlarge", "r7gd.8xlarge", "r7gd.12xlarge", "r7gd.16xlarge",
    "r6gd.large", "r6gd.xlarge", "r6gd.2xlarge", "r6gd.4xlarge", "r6gd.8xlarge", "r6gd.12xlarge", "r6gd.16xlarge"
  ]
}

variable "aurora_writer_instance_types" {
  type        = list(string)
  description = "Graviton (arm64) instance types for the aurora writer compute environment (c7g/c6g/m7g/m6g -- the arm64 counterpart of the c6a/c6i/c5a/c5/c4/m6a/m6i/m5a/m5/m4 families this project used on x86_64). Burstable (t*g) families are intentionally excluded to avoid CPU-credit throttling on the DB writer path."
  default = [
    "c7g.large", "c6g.large",
    "m7g.large", "m6g.large"
  ]
}

variable "api_gateway_usage_plans" {
  type        = map(any)
  description = "Throttling limits for API Gateway"
  default = {
    internal_apps = {
      quota_limit = 5000000 # per day
      burst_limit = 1000
      rate_limit  = 200 # per second
    }
    external_apps = {
      quota_limit = 10000
      burst_limit = 20
      rate_limit  = 10
    }
  }
}

variable "force_delete_ecr_repos" {
  type        = bool
  description = "Whether or not to delete non-empty ECR repos"
  default     = false
}
