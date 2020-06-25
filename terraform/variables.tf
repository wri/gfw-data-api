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

variable "log_retention" { default = 30 }

variable "desired_count" { default = 1 }
variable "fargate_cpu" { default = 256 }
variable "fargate_memory" { default = 2048 }
variable "auto_scaling_cooldown" { default = 300 }
variable "auto_scaling_max_capacity" { default = 15 }
variable "auto_scaling_max_cpu_util" { default = 75 }
variable "auto_scaling_min_capacity" { default = 1 }
variable "key_pair" { default = "tmaschler_gfw" }
variable "git_sha" {}