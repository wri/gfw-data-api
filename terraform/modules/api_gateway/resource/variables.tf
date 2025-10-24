variable "rest_api_id" {
  type = string
}

variable "parent_id" {
  type = string
}

variable "path_part" {
  type = string
}

variable "api_gateway_usage_plans" {
  type        = map(any)
  description = "Throttling limits for API Gateway"
}

variable "service_url" {
  type = string
}
