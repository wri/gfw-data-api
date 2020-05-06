variable "environment" {
  type        = string
  description = "An environment namespace for the infrastructure."
}

variable "region" {
  default = "us-east-1"
  type    = string
}

variable "container_name" { default = "gfw-data-api" }
variable "container_port" { default = 80 }

variable "log_level" {}

variable "log_retention" {default = 30 }