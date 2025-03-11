environment                    = "dev"
log_level                      = "debug"
service_url                    = "https://dev-data-api.globalforestwatch.org" # fake, needed for CloudFront
rw_api_url                     = "https://api.resourcewatch.org"
rw_api_key_arn                 = "arn:aws:secretsmanager:us-east-1:563860007740:secret:gfw-api/rw-api-key-YhLbaM"  # pragma: allowlist secret
desired_count                  = 1
auto_scaling_min_capacity      = 1
auto_scaling_max_capacity      = 5
lambda_analysis_workspace      = "feature-otf_lists"
key_pair                       = "dmannarino_gfw"
create_cloudfront_distribution = false
new_relic_license_key_arn      = "arn:aws:secretsmanager:us-east-1:563860007740:secret:newrelic/license_key-lolw24"
load_balancer_security_group   = "sg-07c9331c01f8da1c8"
load_balancer_arn              = "arn:aws:elasticloadbalancing:us-east-1:563860007740:loadbalancer/app/gfw-data-api-elb-shared-dev-lb/60c3ad42ca6522e3"
lb_dns_name                    = "gfw-data-api-elb-shared-dev-lb-10091095.us-east-1.elb.amazonaws.com"
api_gateway_id                 = "wddlsuo04c"
api_gw_external_app_id         = "f10vmg"
api_gw_internal_app_id         = "ka6k5w"
api_gateway_url                = "https://wddlsuo04c.execute-api.us-east-1.amazonaws.com/deploy"
