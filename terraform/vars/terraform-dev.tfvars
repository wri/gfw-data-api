environment               = "dev"
log_level                 = "debug"
service_url               = "https://dev-data-api.globalforestwatch.org"  # fake, needed for CloudFront
rw_api_url                = "https://staging-api.resourcewatch.org"
desired_count             = 1
auto_scaling_min_capacity = 1
auto_scaling_max_capacity = 5
lambda_analysis_workspace = "features-lat_lon"