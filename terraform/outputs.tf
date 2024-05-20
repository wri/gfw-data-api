output "loadbalancer_dns" {
  value = module.fargate_autoscaling.lb_dns_name
}