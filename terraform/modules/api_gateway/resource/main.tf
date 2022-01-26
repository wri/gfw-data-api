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

  request_templates = {
    "text/plain" : <<EOT
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