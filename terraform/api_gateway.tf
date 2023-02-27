resource "aws_api_gateway_rest_api" "api_gw_api" {
  name           = substr("GFWDataAPIGateway${local.name_suffix}", 0, 64)
  description    = "GFW Data API Gateway"
  api_key_source = "AUTHORIZER" # pragma: allowlist secret

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "dataset_parent" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id   = aws_api_gateway_rest_api.api_gw_api.root_resource_id
  path_part   = "dataset"
}

resource "aws_api_gateway_resource" "dataset" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id   = aws_api_gateway_resource.dataset_parent.id
  path_part   = "{dataset}"
}

resource "aws_api_gateway_resource" "version" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id   = aws_api_gateway_resource.dataset.id
  path_part   = "{version}"
}

resource "aws_api_gateway_resource" "query_parent" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id   = aws_api_gateway_resource.version.id
  path_part   = "query"
}

module "query_resource" {
  source      = "./modules/api_gateway/resource"
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id   = aws_api_gateway_resource.query_parent.id
  path_part   = "{proxy+}"
}

module "query_get" {
  source = "./modules/api_gateway/endpoint"

  rest_api_id   = aws_api_gateway_rest_api.api_gw_api.id
  authorizer_id = aws_api_gateway_authorizer.api_key.id
  api_resource  = module.query_resource.aws_api_gateway_resource

  require_api_key = false
  http_method     = "GET"
  authorization   = "NONE"

  integration_parameters = {
    "integration.request.path.version" = "method.request.path.version"
    "integration.request.path.dataset" = "method.request.path.dataset",
    "integration.request.path.proxy"   = "method.request.path.proxy"
  }

  method_parameters = {
    "method.request.path.dataset" = true,
    "method.request.path.version" = true
    "method.request.path.proxy"   = true

  }

  integration_uri = "http://${module.fargate_autoscaling.lb_dns_name}/dataset/{dataset}/{version}/query/{proxy}"
}

module "query_post" {
  source = "./modules/api_gateway/endpoint"

  rest_api_id   = aws_api_gateway_rest_api.api_gw_api.id
  authorizer_id = aws_api_gateway_authorizer.api_key.id
  api_resource  = module.query_resource.aws_api_gateway_resource

  require_api_key = false
  http_method     = "POST"
  authorization   = "NONE"

  integration_parameters = {
    "integration.request.path.version" = "method.request.path.version"
    "integration.request.path.dataset" = "method.request.path.dataset",
    "integration.request.path.proxy"   = "method.request.path.proxy"
  }

  method_parameters = {
    "method.request.path.dataset" = true,
    "method.request.path.version" = true
    "method.request.path.proxy"   = true

  }

  integration_uri = "http://${module.fargate_autoscaling.lb_dns_name}/dataset/{dataset}/{version}/query/{proxy}"
}

resource "aws_api_gateway_resource" "download_parent" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id   = aws_api_gateway_resource.version.id
  path_part   = "download"
}

module "download_shapes_resources" {
  source = "./modules/api_gateway/resource"

  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id   = aws_api_gateway_resource.download_parent.id

  for_each  = toset(var.download_endpoints)
  path_part = each.key
}

module "download_shapes_endpoint" {
  source = "./modules/api_gateway/endpoint"

  rest_api_id   = aws_api_gateway_rest_api.api_gw_api.id
  authorizer_id = aws_api_gateway_authorizer.api_key.id

  for_each     = module.download_shapes_resources
  api_resource = each.value.aws_api_gateway_resource

  require_api_key = true
  http_method     = "GET"
  authorization   = "CUSTOM"

  integration_parameters = {
    "integration.request.path.dataset" = "method.request.path.dataset",
    "integration.request.path.version" = "method.request.path.version"
  }

  method_parameters = {
    "method.request.path.dataset" = true,
    "method.request.path.version" = true
  }

  integration_uri = "http://${module.fargate_autoscaling.lb_dns_name}/dataset/{dataset}/{version}/download/${each.key}"
}

module unprotected_resource {
  source = "./modules/api_gateway/resource"

  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id   = aws_api_gateway_rest_api.api_gw_api.root_resource_id
  path_part   = "{proxy+}"

}

module "unprotected_endpoints" {
  source = "./modules/api_gateway/endpoint"

  rest_api_id   = aws_api_gateway_rest_api.api_gw_api.id
  authorizer_id = aws_api_gateway_authorizer.api_key.id
  api_resource  = module.unprotected_resource.aws_api_gateway_resource


  require_api_key = false
  http_method     = "ANY"
  authorization   = "NONE"

  method_parameters      = { "method.request.path.proxy" = true }
  integration_parameters = { "integration.request.path.proxy" = "method.request.path.proxy" }

  integration_uri = "http://${module.fargate_autoscaling.lb_dns_name}/{proxy}"
}


resource "aws_api_gateway_usage_plan" "internal" {
  name = substr("internal_apps${local.name_suffix}", 0, 64)

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
  name = substr("external_apps${local.name_suffix}", 0, 64)

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

resource "aws_api_gateway_deployment" "api_gw_dep" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id

  triggers = {
    redeployment = "${md5(file("api_gateway.tf"))}-${md5(file("./modules/api_gateway/endpoint/main.tf"))}-${md5(file("./modules/api_gateway/resource/main.tf"))}"
  }

  depends_on = [
    module.query_get.integration_point,
    module.query_post.integration_point,
    #FIXME don't hardcode the spatial integration points
    module.download_shapes_endpoint["shp"].integration_point,
    module.download_shapes_endpoint["gpkg"].integration_point,
    module.download_shapes_endpoint["geotiff"].integration_point,
    module.unprotected_endpoints.integration_point
  ]

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "api_gw_stage" {
  deployment_id = aws_api_gateway_deployment.api_gw_dep.id
  rest_api_id   = aws_api_gateway_rest_api.api_gw_api.id
  stage_name    = local.api_gw_stage_name
}

# Lambda Authorizer
resource "aws_api_gateway_authorizer" "api_key" {
  name                             = "api_key"
  rest_api_id                      = aws_api_gateway_rest_api.api_gw_api.id
  type                             = "REQUEST"
  authorizer_uri                   = aws_lambda_function.authorizer.invoke_arn
  authorizer_credentials           = aws_iam_role.invocation_role.arn
  authorizer_result_ttl_in_seconds = 0

  # making sure terraform doesn't require default authorization
  # header (https://github.com/hashicorp/terraform-provider-aws/issues/5845)
  identity_source = ","
}


resource "aws_iam_role" "invocation_role" {
  name = substr("api_gateway_auth_invocation${local.name_suffix}", 0, 64)
  path = "/"

  assume_role_policy = data.template_file.api_gateway_role_policy.rendered
}

resource "aws_iam_role_policy" "invocation_policy" {
  name = "default"
  role = aws_iam_role.invocation_role.id

  policy = data.local_file.iam_lambda_invoke.content
}

resource "aws_iam_role" "lambda" {
  name = substr("api_gw_authorizer_lambda${local.name_suffix}", 0, 64)

  assume_role_policy = data.template_file.lambda_role_policy.rendered
}

resource "aws_lambda_function" "authorizer" {
  filename      = "api_gateway/api_key_authorizer_lambda.zip"
  function_name = substr("api_gateway_authorizer${local.name_suffix}", 0, 64)
  runtime       = "python3.8"
  role          = aws_iam_role.lambda.arn
  handler       = "lambda_function.handler"

  source_code_hash = filebase64sha256("api_gateway/api_key_authorizer_lambda.zip")

  depends_on = [
    aws_iam_role.cloudwatch
  ]
}


# Cloudwatch Logging
resource "aws_api_gateway_account" "main" {
  cloudwatch_role_arn = aws_iam_role.cloudwatch.arn
}

resource "aws_iam_role" "cloudwatch" {
  name = substr("api_gateway_cloudwatch_global${local.name_suffix}", 0, 64)

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
    metrics_enabled    = true
    data_trace_enabled = true
    logging_level      = "INFO"
  }

  depends_on = [
    aws_iam_role.cloudwatch
  ]
}
