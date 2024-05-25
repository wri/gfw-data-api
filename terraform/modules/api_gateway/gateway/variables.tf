variable "name" {
  type        = string
  description = "Name of API Gateway instance"
  default     = "GFWDataAPIGateway"
}

variable "description" {
  type        = string
  description = "Description of API Gateway Instance"
  default     = "GFW Data API Gateway"
}

variable "stage_name" {
  type        = string
  description = "The stage under which the instance will be deployed"
  default     = "deploy"
}

variable "download_endpoints" {
  type        = list(string)
  description = "path parts to download endpoints"

  # listing spatial endpoints as gateway needs them explicitly created
  # in order to apply endpoint-level throttling to them
  default = ["geotiff", "gpkg", "shp"]
}

variable "lb_dns_name" {
  type        = string
  description = "Application load balancer to forward requests to"
}

variable "api_gateway_role_policy" {
  type = string
}

variable "lambda_role_policy" {
  type = string
}

variable "cloudwatch_policy" {
  type = string
}

variable "lambda_invoke_policy" {
  type = string
}

variable "api_gateway_usage_plans" {
  type        = map(any)
  description = "Throttling limits for API Gateway"
  default = {
    internal_apps = {
      quota_limit = 50000 # per day
      burst_limit = 1000
      rate_limit  = 200 # per second
    }
    external_apps = {
      quota_limit = 1000
      burst_limit = 20
      rate_limit  = 10
    }
  }
}
