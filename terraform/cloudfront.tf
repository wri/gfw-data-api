resource "aws_cloudfront_distribution" "data_api" {
  enabled             = true
  is_ipv6_enabled     = true
  price_class         = "PriceClass_All"
  aliases             = var.environment == "dev" ? null : [replace(var.service_url, "https://", "")]

 origin {
    domain_name = module.fargate_autoscaling.lb_dns_name
    custom_origin_config {
      http_port = 80
      https_port = 443
      origin_keepalive_timeout = 5
      origin_protocol_policy = "http-only"
      origin_read_timeout = 30
      origin_ssl_protocols = [
        "TLSv1",
        "TLSv1.1",
        "TLSv1.2",
      ]
    }
    origin_id = "load_balancer"
  }

  ordered_cache_behavior {
    path_pattern = "/"
    target_origin_id       = "load_balancer"
    default_ttl            = 0
    min_ttl                = 0
    max_ttl                = 31536000 # 1y

    allowed_methods = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods  = ["GET", "HEAD"]
    viewer_protocol_policy = "redirect-to-https"

    forwarded_values {
      headers                 = ["Origin", "Access-Control-Request-Headers", "Access-Control-Request-Method", "x-api-key", "Referer", "Authorization"]
      query_string            = true

      cookies {
        forward = "none"
        whitelisted_names = []
      }
    }
  }

  ordered_cache_behavior {
    path_pattern = "/openapi.json"
    target_origin_id       = "load_balancer"
    default_ttl            = 0
    min_ttl                = 0
    max_ttl                = 31536000 # 1y

    allowed_methods = ["GET", "HEAD", "OPTIONS"]
    cached_methods  = ["GET", "HEAD"]
    viewer_protocol_policy = "redirect-to-https"

    forwarded_values {
      headers                 = ["Origin", "Access-Control-Request-Headers", "Access-Control-Request-Method", "x-api-key", "Referer", "Authorization"]
      query_string            = true

      cookies {
        forward = "none"
        whitelisted_names = []
      }
    }
  }

  origin {
    domain_name = trimsuffix(trimprefix(aws_api_gateway_stage.api_gw_stage.invoke_url, "https://"), "/${local.api_gw_stage_name}")
    custom_origin_config {
      http_port = 80
      https_port = 443
      origin_keepalive_timeout = 5
      origin_protocol_policy = "https-only"
      origin_read_timeout = 30
      origin_ssl_protocols = [
        "TLSv1",
        "TLSv1.1",
        "TLSv1.2",
      ]
    }
    origin_id = "api_gateway"
    origin_path = "/${local.api_gw_stage_name}"
  }

  default_cache_behavior {
    target_origin_id       = "api_gateway"
    default_ttl            = 0
    min_ttl                = 0
    max_ttl                = 31536000 # 1y

    allowed_methods = ["GET", "HEAD", "OPTIONS", "PUT", "POST", "PATCH", "DELETE"]
    cached_methods  = ["GET", "HEAD"]
    viewer_protocol_policy = "redirect-to-https"

    forwarded_values {
      headers                 = ["Origin", "Access-Control-Request-Headers", "Access-Control-Request-Method", "x-api-key", "Referer", "Authorization"]
      query_string            = true

      cookies {
        forward = "none"
        whitelisted_names = []
      }
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn            = data.terraform_remote_state.core.outputs.acm_certificate
    cloudfront_default_certificate = false
    minimum_protocol_version       = "TLSv1.1_2016"
    ssl_support_method             = "sni-only"
  }
}
