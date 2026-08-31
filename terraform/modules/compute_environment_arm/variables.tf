variable "project" {
  type = string
}

variable "key_pair" {
  type = string
}

variable "subnets" {
  type = list(string)
}

variable "tags" {
  type = map(string)
}

variable "security_group_ids" {
  type = list(string)
}

variable "ecs_role_policy_arns" {
  type = list(string)
}

variable "suffix" {
  type    = string
  default = ""
}

variable "instance_types" {
  type = list(string)
  # Graviton (arm64) equivalents of the vendored module's default x86_64
  # NVMe-backed families (r5d/c5d) -- r/c *gd* families.
  default = [
    "r7gd.4xlarge", "r7gd.8xlarge", "r7gd.12xlarge", "r7gd.16xlarge",
    "c7gd.12xlarge", "c7gd.18xlarge", "c7gd.24xlarge"
  ]
}

variable "use_ephemeral_storage" {
  type    = bool
  default = true
}

variable "ebs_volume_size" {
  type    = number
  default = 30
}

variable "bid_percentage" {
  type    = number
  default = 100
}

variable "max_vcpus" {
  type    = number
  default = 256
}

variable "min_vcpus" {
  type    = number
  default = 0
}

variable "launch_type" {
  type    = string
  default = "SPOT"
}

variable "compute_environment_name" {
  type    = string
  default = "ephemeral_storage_arm"
}
