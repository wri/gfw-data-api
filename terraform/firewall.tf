# ALB Security group
# This is the group you need to edit if you want to restrict access to your application
resource "aws_security_group" "egress_https" {
  name        = "${local.project}-sg_egress_https${local.name_suffix}"
  description = "Allow outgoing HTTPS traffic"
  vpc_id      = data.terraform_remote_state.core.outputs.vpc_id

  ingress {
    protocol    = "tcp"
    from_port   = 443
    to_port     = 443
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    {
      Name = "${local.project}-sg_egress_https${local.name_suffix}"
    },
    local.tags
  )
}