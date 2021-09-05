resource "aws_api_gateway_rest_api" "api_gw_api" {
  name = "GFWDataAPIGateway"
  description = "GFW Data API Gateway"
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
  name         = "internal_users"

  # api_stages {
  #   api_id = aws_api_gateway_rest_api.api_gw_api.id
  #   stage  = aws_api_gateway_stage.dev.stage_name
  # }

  quota_settings {
    limit  = 10000 # TODO: change to variable
    period = "DAY"
  }

  throttle_settings {
    burst_limit = 100 # TODO: change to variable
    rate_limit  = 500 # per second
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
  rest_api_id = aws_api_gateway_rest_api.api_gw_api.id
}

resource "aws_api_gateway_stage" "api_gw_stage" {
  deployment_id = aws_api_gateway_deployment.api_gw_dep.id
  rest_api_id   = aws_api_gateway_rest_api.api_gw_api.id
  stage_name    = local.api_gw_stage_name
}
