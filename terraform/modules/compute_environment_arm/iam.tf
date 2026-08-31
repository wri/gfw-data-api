# ECS Instance Role

resource "aws_iam_role" "ecs_instance_role" {
  name               = substr("${var.project}-${var.compute_environment_name}-ecs_instance_role${var.suffix}", 0, 64)
  assume_role_policy = data.template_file.ec2_trust_policy.rendered
}

resource "aws_iam_instance_profile" "ecs_instance_role" {
  name = substr("${var.project}-${var.compute_environment_name}-ecs_instance_role${var.suffix}", 0, 64)
  role = aws_iam_role.ecs_instance_role.name
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role_ec2_contantainer_service" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "role-policy-attachment" {
  role       = aws_iam_role.ecs_instance_role.name
  count      = length(var.ecs_role_policy_arns)
  policy_arn = var.ecs_role_policy_arns[count.index]
}


# ECS Spot fleet role

resource "aws_iam_role" "ec2_spot_fleet_role" {
  name               = substr("${var.project}-${var.compute_environment_name}-ec2_spot_fleet_role${var.suffix}", 0, 64)
  assume_role_policy = data.template_file.spotfleet_trust_policy.rendered
}

resource "aws_iam_role_policy_attachment" "ec2_spotfleet_tagging" {
  role       = aws_iam_role.ec2_spot_fleet_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole"
}

# ECS Batch service role

resource "aws_iam_role" "aws_batch_service_role" {
  name               = substr("${var.project}-${var.compute_environment_name}-aws_batch_service_role${var.suffix}", 0, 64)
  assume_role_policy = data.template_file.batch_trust_policy.rendered
}

resource "aws_iam_role_policy_attachment" "aws_batch_service_role" {
  role       = aws_iam_role.aws_batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

# also add ecs role policies to batch service role
resource "aws_iam_role_policy_attachment" "aws_batch_service_role_extended" {
  role       = aws_iam_role.aws_batch_service_role.name
  count      = length(var.ecs_role_policy_arns)
  policy_arn = var.ecs_role_policy_arns[count.index]
}
