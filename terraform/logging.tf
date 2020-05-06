#
# CloudWatch Resources
#
resource "aws_cloudwatch_log_group" "default" {
  name              = "/aws/ecs/${local.project}-log${local.name_suffix}"
  retention_in_days = var.log_retention
}
