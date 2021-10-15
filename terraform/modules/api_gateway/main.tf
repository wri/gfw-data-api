resource "aws_api_gateway_resource" "resource" {
  rest_api_id = var.rest_api_id
  parent_id = var.parent_id
  path_part = var.path_part
}

resource "aws_api_gateway_method" "method" {
  rest_api_id = var.rest_api_id
  resource_id = aws_api_gateway_resource.resource.id
  http_method = var.http_method
  authorization = var.authorization
  authorizer_id = var.authorizer_id
  request_parameters = {
    "method.request.path.proxy" = true
    }
  api_key_required = var.require_api_key
}


resource "aws_api_gateway_integration" "integration" {
  rest_api_id = var.rest_api_id
  resource_id = aws_api_gateway_resource.resource.id
  http_method = aws_api_gateway_method.method.http_method


  integration_http_method = "ANY"
  type = "HTTP_PROXY"
  uri = "http://${var.load_balancer_name}/{proxy}"

  request_parameters = {
    "integration.request.path.proxy" = "method.request.path.proxy"
  }
}