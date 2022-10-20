resource "aws_api_gateway_resource" "aws_api_gateway_resource" {
  rest_api_id = var.rest_api_id
  parent_id   = var.parent_id
  path_part   = var.path_part
}

resource "aws_api_gateway_method" "get_endpoint_method" {
  rest_api_id   = var.rest_api_id
  resource_id   = aws_api_gateway_resource.aws_api_gateway_resource.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "get_endpoint_integration" {
  rest_api_id = var.rest_api_id
  resource_id = aws_api_gateway_resource.aws_api_gateway_resource.id
  http_method = aws_api_gateway_method.get_endpoint_method.http_method
  type        = "MOCK"

  passthrough_behavior    = "WHEN_NO_MATCH"
  request_templates       = {
    "application/json" : <<EOT
    {'statusCode': 200}
    #set($context.responseOverride.header.Access-Control-Allow-Origin = $input.params('origin'))
    EOT
  }
  depends_on = [
  aws_api_gateway_method.get_endpoint_method]
}

resource "aws_api_gateway_method_response" "get_endpoint_method_response" {
  rest_api_id = var.rest_api_id
  resource_id = aws_api_gateway_resource.aws_api_gateway_resource.id
  http_method = aws_api_gateway_method.get_endpoint_method.http_method
  status_code = 200

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" : true
    "method.response.header.Access-Control-Allow-Methods" : true
    "method.response.header.Access-Control-Allow-Origin" : true
    "method.response.header.Access-Control-Allow-Credentials" : true
  }
}

resource "aws_api_gateway_integration_response" "get_endpoint_integration_response" {
  rest_api_id = var.rest_api_id
  resource_id = aws_api_gateway_resource.aws_api_gateway_resource.id
  http_method = aws_api_gateway_method.get_endpoint_method.http_method
  status_code = aws_api_gateway_method_response.get_endpoint_method_response.status_code

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" : "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,upgrade-insecure-requests'"
    "method.response.header.Access-Control-Allow-Methods" : "'OPTIONS,GET,PUT,POST,PATCH,DELETE'"
    "method.response.header.Access-Control-Allow-Credentials" : "'true'"
  }
}

resource "aws_api_gateway_gateway_response" "invalid_api_key" {
  rest_api_id   = var.rest_api_id
  status_code   = "403"
  response_type = "INVALID_API_KEY"

  response_templates = {
    "application/json" = "{\"status\":\"failed\",\"message\":\"Request is missing valid API key. Please see documentation at https://data-api.globalforestwatch.org/#tag/Authentication on how to create one.\"}"
  }
}

resource "aws_api_gateway_gateway_response" "exceeded_quota" {
  rest_api_id   = var.rest_api_id
  status_code   = "429"
  response_type = "QUOTA_EXCEEDED"

  response_templates = {
    "application/json" = "{\"status\":\"failed\",\"message\":\"Exceeded the daily quota for this resource.\"}"
  }
}

resource "aws_api_gateway_gateway_response" "throttled" {
  rest_api_id   = var.rest_api_id
  status_code   = "429"
  response_type = "THROTTLED"

  response_templates = {
    "application/json" = "{\"status\":\"failed\",\"message\":\"Exceeded the rate limit for this resource. Please try again later.\"}"
  }
}

resource "aws_api_gateway_gateway_response" "integration_timeout" {
  rest_api_id = var.rest_api_id
  status_code = "504"
  response_type = "INTEGRATION_TIMEOUT"

  response_templates = {
    "application/json" = <<EOF
{
  "status": "failed",
  "message": "$context.error.messageString : Use the pagination query parameters when available. See the API documentation."
}
EOF
  }
}
