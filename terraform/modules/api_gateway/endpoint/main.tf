resource "aws_api_gateway_method" "method" {
  rest_api_id = var.rest_api_id
  resource_id = var.api_resource.id
  http_method = var.http_method
  authorization = var.authorization
  authorizer_id = var.authorizer_id
  request_parameters = var.method_parameters
  api_key_required = var.require_api_key
}


resource "aws_api_gateway_integration" "integration" {
  rest_api_id = var.rest_api_id
  resource_id = var.api_resource.id
  http_method = aws_api_gateway_method.method.http_method


  integration_http_method = "ANY"
  type = "HTTP_PROXY"
  uri = var.integration_uri

  request_parameters = var.integration_parameters
}