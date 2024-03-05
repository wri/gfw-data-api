output "internal_usage_plan_id" {
  value = aws_api_gateway_usage_plan.internal.id
}

output "external_usage_plan_id" {
  value = aws_api_gateway_usage_plan.external.id
}

output "api_gateway_id" {
  value = aws_api_gateway_rest_api.api_gw_api.id
}

output "invoke_url" {
  value = aws_api_gateway_stage.api_gw_stage.invoke_url
}
