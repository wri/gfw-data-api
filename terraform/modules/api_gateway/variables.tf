variable "rest_api_id" {
  type        = string
  description = "Id of API Gateway to add resource to"
}

variable "authorizer_id" {
    type    = string
    default = ""
}
variable "parent_id" {
  type        = string
  description = "Id of parent resource"
}

variable "require_api_key" {
  type    = bool
  default = false
}

variable "http_method" {
  type = string

  validation {
    condition = contains([
      "ANY",
      "DELETE",
      "GET",
      "HEAD",
      "OPTIONS",
      "PATCH",
      "POST",
      "PUT"
    ], var.http_method)
    error_message = "Invalid HTTP method passed."
  }
}

variable "path_part" {
  type = string
}

variable "authorization" {
  validation {
    condition = contains([
      "NONE",
      "CUSTOM",
      "AWS_IAM",
      "COGNITO_USER_POOLS"
    ], var.authorization)
    error_message = "Unknown authorization method."
  }
}

variable "integration_uri" {
  type = string
}

variable "integration_parameters" {
  type = map
}

variable "method_parameters" {
  type = map
}
