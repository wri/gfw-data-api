resource "aws_api_gateway_rest_api" "api_gw_api" {
  name = "GFWDataAPIGateway"
  description = "GFW Data API Gateway"
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
resource "aws_api_gateway_resource" "query" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.query_parent.id
  path_part = "{proxy+}"
}

resource "aws_api_gateway_method" "query" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.query.id
  http_method = "ANY"
  authorization = "NONE"
  request_parameters = {"method.request.path.proxy" = true}
  api_key_required = true
}


resource "aws_api_gateway_integration" "query" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.query.id
  http_method = aws_api_gateway_method.query.http_method

  integration_http_method = "ANY"
  type = "HTTP_PROXY"
  uri = "http://${module.fargate_autoscaling.lb_dns_name}/{proxy}"

  request_parameters = {
    "integration.request.path.proxy" = "method.request.path.proxy"
  }
}

resource "aws_api_gateway_resource" "download_parent" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.version.id
  path_part = "download"
}

resource "aws_api_gateway_resource" "download_shp" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.download_parent.id
  path_part = "shp"
}

resource "aws_api_gateway_method" "download_shp" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.download_shp.id
  http_method = "GET"
  authorization = "NONE"
  request_parameters = {
    "method.request.path.dataset" = true,
    "method.request.path.version" = true}
  api_key_required = true
}


resource "aws_api_gateway_integration" "download_shp" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.download_shp.id
  http_method = aws_api_gateway_method.download_shp.http_method

  integration_http_method = "ANY"
  type = "HTTP_PROXY"
  uri = "http://${module.fargate_autoscaling.lb_dns_name}/dataset/{dataset}/{version}/download/shp"

  request_parameters = {
    "integration.request.path.dataset" = "method.request.path.dataset",
    "integration.request.path.version" = "method.request.path.version"
  }
}

resource "aws_api_gateway_resource" "download_gpkg" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.download_parent.id
  path_part = "gpkg"
}

resource "aws_api_gateway_method" "download_gpkg" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.download_gpkg.id
  http_method = "GET"
  authorization = "NONE"
  request_parameters = {
    "method.request.path.dataset" = true,
    "method.request.path.version" = true}
  api_key_required = true
}

resource "aws_api_gateway_integration" "download_gpkg" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.download_gpkg.id
  http_method = aws_api_gateway_method.download_gpkg.http_method

  integration_http_method = "ANY"
  type = "HTTP_PROXY"
  uri = "http://${module.fargate_autoscaling.lb_dns_name}/dataset/{dataset}/{version}/download/gpkg"

  request_parameters = {
    "integration.request.path.dataset" = "method.request.path.dataset",
    "integration.request.path.version" = "method.request.path.version"
  }
}

resource "aws_api_gateway_resource" "download_geotiff" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_resource.download_parent.id
  path_part = "geotiff"
}

resource "aws_api_gateway_method" "download_geotiff" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.download_geotiff.id
  http_method = "GET"
  authorization = "NONE"
  request_parameters = {
    "method.request.path.dataset" = true,
    "method.request.path.version" = true}
  api_key_required = true
}

resource "aws_api_gateway_integration" "download_geotiff" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.download_geotiff.id
  http_method = aws_api_gateway_method.download_geotiff.http_method

  integration_http_method = "ANY"
  type = "HTTP_PROXY"
  uri = "http://${module.fargate_autoscaling.lb_dns_name}/dataset/{dataset}/{version}/download/geotiff"

  request_parameters = {
    "integration.request.path.dataset" = "method.request.path.dataset",
    "integration.request.path.version" = "method.request.path.version"
  }
}


resource "aws_api_gateway_resource" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  parent_id = aws_api_gateway_rest_api.api_gw_api.root_resource_id
  path_part = "{proxy+}"
}

resource "aws_api_gateway_method" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.proxy.id
  http_method = "ANY"
  authorization = "NONE"
  request_parameters = {"method.request.path.proxy" = true}
  api_key_required = false
}


resource "aws_api_gateway_integration" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
  resource_id = aws_api_gateway_resource.proxy.id
  http_method = aws_api_gateway_method.proxy.http_method

  integration_http_method = "ANY"
  type = "HTTP_PROXY"
  uri = "http://${module.fargate_autoscaling.lb_dns_name}/{proxy}"

  request_parameters = {
    "integration.request.path.proxy" = "method.request.path.proxy"
  }
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
}

resource "aws_api_gateway_usage_plan_key" "internal" {
  key_id        = aws_api_gateway_api_key.internal_api_key.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.internal.id
}

resource "aws_api_gateway_deployment" "api_gw_dep" {
  depends_on = [
    aws_api_gateway_integration.proxy,
  ]
  lifecycle {
    create_before_destroy = true
  }
  # force api stage reploy if file changes
  stage_description = md5(file("api_gateway.tf"))
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
}

resource "aws_api_gateway_stage" "api_gw_stage" {
  deployment_id = aws_api_gateway_deployment.api_gw_dep.id
  rest_api_id   = aws_api_gateway_rest_api.api_gw_api.id
  stage_name    = local.api_gw_stage_name
}

# Cloudwatch Logging
resource "aws_api_gateway_account" "main" {
  cloudwatch_role_arn = aws_iam_role.cloudwatch.arn
}

resource "aws_iam_role" "cloudwatch" {
  name = "api_gateway_cloudwatch_global"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "apigateway.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "cloudwatch" {
  name = "default"
  role = aws_iam_role.cloudwatch.id

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:PutLogEvents",
                "logs:GetLogEvents",
                "logs:FilterLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
EOF
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