environment               = "staging"
log_level                 = "info"
service_url               = "https://staging-data-api.globalforestwatch.org"
rw_api_url                = "https://api.resourcewatch.org"
rw_api_key_arn            = "arn:aws:secretsmanager:us-east-1:274931322839:secret:gfw-api/rw-api-key-xG9YwX"  # pragma: allowlist secret
desired_count             = 1
auto_scaling_min_capacity = 1
auto_scaling_max_capacity = 15
lambda_analysis_workspace = "default"
key_pair                  = "dmannarino_gfw"
new_relic_license_key_arn = "arn:aws:secretsmanager:us-east-1:274931322839:secret:newrelic/license_key-1wKZAY"
