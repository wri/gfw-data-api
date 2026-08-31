data "aws_ami" "latest-amazon-ecs-optimized-arm" {

  most_recent = true
  owners      = ["591542846629"] # AWS

  # The vendored (x86_64) compute_environment module in gfw-terraform-modules
  # filters on the legacy Amazon Linux 1 "amzn-ami-*-amazon-ecs-optimized"
  # name pattern, which was never published for arm64. The ECS-optimized
  # Amazon Linux 2 arm64 AMI uses this naming instead.
  filter {
    name   = "name"
    values = ["amzn2-ami-ecs-hvm-*-arm64-ebs"]
  }
  filter {
    name   = "architecture"
    values = ["arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "template_file" "batch_trust_policy" {
  template = file("${path.module}/templates/role-trust-policy.json")
  vars = {
    service = "batch"
  }
}

data "template_file" "ec2_trust_policy" {
  template = file("${path.module}/templates/role-trust-policy.json")
  vars = {
    service = "ec2"
  }
}

data "template_file" "spotfleet_trust_policy" {
  template = file("${path.module}/templates/role-trust-policy.json")
  vars = {
    service = "spotfleet"
  }
}

data "local_file" "mount_tmp_enable_swap" {
  filename = "${path.module}/user_data/mount_tmp_enable_swap.sh"
}
