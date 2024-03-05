output "loadbalancer_dns" {
  value = coalesce(module.fargate_autoscaling.lb_dns_name, var.lb_dns_name)
}

output "generated_port" {
  value = length(data.external.generate_port) > 0 ? data.external.generate_port[0].result["port"] : var.listener_port
}
