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

variable "api_gw_internal_up_id" {
  type        = string
  description = "ID of API Gateway usage plan for internal domains"
  default     = ""
}

variable "api_gw_external_up_id" {
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
  description = "memory optimized EC2 instances with local NVMe SSDs for data lake writer batche queues"
  default = [
    "r6id.large", "r6id.xlarge", "r6id.2xlarge", "r6id.4xlarge", "r6id.8xlarge", "r6id.12xlarge", "r6id.16xlarge", "r6id.24xlarge",
    "r5ad.large", "r5ad.xlarge", "r5ad.2xlarge", "r5ad.4xlarge", "r5ad.8xlarge", "r5ad.12xlarge", "r5ad.16xlarge", "r5ad.24xlarge",
    "r5d.large", "r5d.xlarge", "r5d.2xlarge", "r5d.4xlarge", "r5d.8xlarge", "r5d.12xlarge", "r5d.16xlarge", "r5d.24xlarge"
  ]
}
