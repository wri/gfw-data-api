environment               = "production"
log_level                 = "info"
service_url               = "https://data-api.globalforestwatch.org"
rw_api_url                = "https://api.resourcewatch.org"
rw_api_key_arn            = "arn:aws:secretsmanager:us-east-1:401951483516:secret:gfw-api/rw-api-key-YQ50uP"  # pragma: allowlist secret
desired_count             = 2
auto_scaling_min_capacity = 2
auto_scaling_max_capacity = 15
fargate_cpu               = 2048
fargate_memory            = 4096
lambda_analysis_workspace = "default"
key_pair                  = "dmannarino_gfw"
new_relic_license_key_arn = "arn:aws:secretsmanager:us-east-1:401951483516:secret:newrelic/license_key-CyqUPX"
