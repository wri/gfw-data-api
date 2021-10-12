resource "aws_api_gateway_rest_api" "api_gw_api" {
  name = "GFWDataAPIGateway"
  description = "GFW Data API Gateway"
  api_key_source = "AUTHORIZER"
}

resource "aws_api_gateway_resource" "dataset_parent" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_rest_api.api_gw_api.root_resource_id
  path_part = "dataset"
}

resource "aws_api_gateway_resource" "dataset" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.dataset_parent.id
  path_part = "{dataset}"
}

resource "aws_api_gateway_resource" "version" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.dataset.id
  path_part = "{version}"
}

resource "aws_api_gateway_resource" "query_parent" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.version.id
  path_part = "query"
}

module "query" {
  source = "./modules/api_gateway"

  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  authorizer_id = aws_api_gateway_authorizer.api_key.id
  parent_id = aws_api_gateway_resource.query_parent.id

  require_api_key = true
  http_method = "ANY"
  path_part = "{proxy+}"
  authorization = "CUSTOM"

  load_balancer_name = module.fargate_autoscaling.lb_dns_name
}
resource "aws_api_gateway_resource" "download_parent" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.version.id
  path_part = "download"
}

module "download_shapes" {
  source = "./modules/api_gateway"

  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  authorizer_id = aws_api_gateway_authorizer.api_key.id
  parent_id = aws_api_gateway_resource.download_parent.id

  for_each = toset(var.download_endpoints)

  require_api_key = true
  http_method = "GET"
  path_part = each.key
  authorization = "CUSTOM"

  load_balancer_name = module.fargate_autoscaling.lb_dns_name
}

module "unprotected_paths" {
  source = "./modules/api_gateway"

  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  authorizer_id = aws_api_gateway_authorizer.api_key.id
  parent_id = aws_api_gateway_rest_api.api_gw_api.root_resource_id

  require_api_key = false
  http_method = "ANY"
  path_part = "{proxy+}"
  authorization = "NONE"

  load_balancer_name = module.fargate_autoscaling.lb_dns_name
}

resource "aws_api_gateway_api_key" "internal_api_key" {
  name = "internal"
}

resource "aws_api_gateway_usage_plan" "internal" {
  name         = "internal_apps"

  api_stages {
    api_id = aws_api_gateway_rest_api.api_gw_api.id
    stage  = aws_api_gateway_stage.api_gw_stage.stage_name
  }

  quota_settings {
    limit  = var.api_gateway_usage_plans.internal_apps.quota_limit
    period = "DAY"
  }

  throttle_settings {
    burst_limit = var.api_gateway_usage_plans.internal_apps.burst_limit
    rate_limit  = var.api_gateway_usage_plans.internal_apps.rate_limit
  }

  # terraform doesn't expose API Gateway's method level throttling so will do that
  # manually and this will stop terraform from destroying the manual changes
  # Open PR to add the feature to terraform: https://github.com/hashicorp/terraform-provider-aws/pull/20672
  lifecycle {
    ignore_changes = all
  }
}

resource "aws_api_gateway_usage_plan" "external" {
  name         = "external_apps"

  api_stages {
    api_id = aws_api_gateway_rest_api.api_gw_api.id
    stage  = aws_api_gateway_stage.api_gw_stage.stage_name
  }

  quota_settings {
    limit  = var.api_gateway_usage_plans.external_apps.quota_limit
    period = "DAY"
  }

  throttle_settings {
    burst_limit = var.api_gateway_usage_plans.external_apps.burst_limit
    rate_limit  = var.api_gateway_usage_plans.external_apps.rate_limit
  }

  # terraform doesn't expose API Gateway's method level throttling so will do that
  # manually and this will stop terraform from destroying the manual changes
  # Open PR to add the feature to terraform: https://github.com/hashicorp/terraform-provider-aws/pull/20672
  lifecycle {
    ignore_changes = all
  }

}

resource "aws_api_gateway_usage_plan_key" "internal" {
  key_id        = aws_api_gateway_api_key.internal_api_key.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.internal.id
}

resource "aws_api_gateway_deployment" "api_gw_dep" {
  depends_on = [
    module.unprotected_paths.integration_point,
  ]
  lifecycle {
    create_before_destroy = true
  }
  # force api stage reploy if file changes
  stage_description = "${md5(file("api_gateway.tf"))}-${md5(file("./modules/api_gateway/main.tf"))}"
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
}

resource "aws_api_gateway_stage" "api_gw_stage" {
  deployment_id = aws_api_gateway_deployment.api_gw_dep.id
  rest_api_id   = aws_api_gateway_rest_api.api_gw_api.id
  stage_name    = local.api_gw_stage_name
}

# Lambda Authorizer
resource "aws_api_gateway_authorizer" "api_key" {
  name                   = "api_key"
  rest_api_id            = aws_api_gateway_rest_api.api_gw_api.id
  type                   = "REQUEST"
  authorizer_uri         = aws_lambda_function.authorizer.invoke_arn
  authorizer_credentials = aws_iam_role.invocation_role.arn

  # making sure terraform doesn't require default authorization
  # header (https://github.com/hashicorp/terraform-provider-aws/issues/5845)
  identity_source        = ","
}


resource "aws_iam_role" "invocation_role" {
  name = "api_gateway_auth_invocation"
  path = "/"

  assume_role_policy = data.template_file.api_gateway_role_policy.rendered
}

resource "aws_iam_role_policy" "invocation_policy" {
  name = "default"
  role = aws_iam_role.invocation_role.id

  policy = data.local_file.iam_lambda_invoke.content
}



resource "aws_iam_role" "lambda" {
  name = "api_gw_authorizer_lambda"

  assume_role_policy = data.template_file.lambda_role_policy.rendered
}

resource "aws_lambda_function" "authorizer" {
  filename      = "api_gateway/api_key_authorizer_lambda.zip"
  function_name = "api_gateway_authorizer"
  runtime       = "python3.8"
  role          = aws_iam_role.lambda.arn
  handler       = "lambda_function.handler"

  source_code_hash = filebase64sha256("api_gateway/api_key_authorizer_lambda.zip")

  depends_on =[
    aws_iam_role.cloudwatch
  ]
}


# Cloudwatch Logging
resource "aws_api_gateway_account" "main" {
  cloudwatch_role_arn = aws_iam_role.cloudwatch.arn
}

resource "aws_iam_role" "cloudwatch" {
  name = "api_gateway_cloudwatch_global"

  assume_role_policy = data.template_file.api_gateway_role_policy.rendered
}



resource "aws_iam_role_policy" "api_gw_cloudwatch" {
  name = "default"
  role = aws_iam_role.cloudwatch.id

  policy = data.local_file.cloudwatch_log_policy.content
}

resource "aws_iam_role_policy" "lambda_cloudwatch" {
  name = "default"
  role = aws_iam_role.lambda.id

  policy = data.local_file.cloudwatch_log_policy.content
}

resource "aws_api_gateway_method_settings" "general_settings" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  stage_name  = aws_api_gateway_stage.api_gw_stage.stage_name
  method_path = "*/*"

  settings {
    # Enable CloudWatch logging and metrics
    metrics_enabled        = true
    data_trace_enabled     = true
    logging_level          = "INFO"
  }
}
