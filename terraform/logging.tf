#
# CloudWatch Resources
#
resource "aws_cloudwatch_log_group" "default" {
  name              = substr("/aws/ecs/${local.project}-log${local.name_suffix}", 0, 64)
  retention_in_days = var.log_retention
}
