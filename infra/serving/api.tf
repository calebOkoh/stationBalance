###############################################################################
# API Gateway — drawio `api`, "HTTP API | /stations /forecast /attribution"
#
# HTTP API rather than REST: $1.00 per million requests against $3.50, and
# none of the REST-only features (request validators, VTL, usage plans) are
# wanted here.
###############################################################################
resource "aws_apigatewayv2_api" "public" {
  name          = var.service
  protocol_type = "HTTP"
  description   = "Indego station capacity — precomputed forecasts and live dock counts"

  cors_configuration {
    allow_origins = var.cors_allow_origins
    allow_methods = ["GET", "OPTIONS"]
    allow_headers = ["content-type"]
    max_age       = 3600
  }

  tags = {
    step = "serving"
  }
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.public.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
  timeout_milliseconds   = 10000
}

# Route keys are matched verbatim by the handler's ROUTES table, so adding a
# route here without adding it there returns a 404 rather than a 500.
resource "aws_apigatewayv2_route" "routes" {
  for_each = toset([
    "GET /stations",
    "GET /forecast",
    "GET /attribution",
  ])

  api_id    = aws_apigatewayv2_api.public.id
  route_key = each.value
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_cloudwatch_log_group" "api_gateway" {
  name              = "/aws/apigateway/${var.service}"
  retention_in_days = var.log_retention_days
  tags              = { step = "serving" }
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.public.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.api_gateway.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      httpMethod     = "$context.httpMethod"
      path           = "$context.path"
      status         = "$context.status"
      responseLength = "$context.responseLength"
      latency        = "$context.responseLatency"
      integrationErr = "$context.integrationErrorMessage"
    })
  }

  # A public unauthenticated endpoint over free public data. Throttling is what
  # stops a scraper turning a $0 bill into a real one; it is not a security
  # control.
  default_route_settings {
    throttling_burst_limit = 100
    throttling_rate_limit  = 50
  }

  tags = {
    step = "serving"
  }
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.public.execution_arn}/*/*"
}

###############################################################################
# CloudFront — drawio `cf`, "global edge | 60 s TTL"
#
# The cube is regenerated hourly and the live snapshot every minute, so a 60 s
# edge TTL is free accuracy-wise and collapses repeat map loads onto one origin
# request. It also puts a cache in front of the throttle above, so a burst of
# identical requests never reaches API Gateway at all.
#
# PriceClass_100 (North America and Europe): the tool is a Philadelphia
# bikeshare map.
###############################################################################
locals {
  api_origin_domain = replace(aws_apigatewayv2_api.public.api_endpoint, "https://", "")
}

resource "aws_cloudfront_cache_policy" "api" {
  name        = "${var.service}-api"
  default_ttl = 60
  min_ttl     = 0
  max_ttl     = 300

  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = true

    # station_id is part of the cache key, or every station would collide on
    # one cached /forecast response.
    query_strings_config {
      query_string_behavior = "whitelist"
      query_strings {
        items = ["station_id"]
      }
    }

    # Host must NOT be forwarded to an API Gateway origin: the origin rejects
    # a Host header that is not its own domain.
    headers_config {
      header_behavior = "none"
    }

    cookies_config {
      cookie_behavior = "none"
    }
  }
}

resource "aws_cloudfront_distribution" "api" {
  enabled         = true
  is_ipv6_enabled = true
  comment         = "${var.service} — edge cache in front of the HTTP API"
  price_class     = "PriceClass_100"

  origin {
    domain_name = local.api_origin_domain
    origin_id   = "apigw-${aws_apigatewayv2_api.public.id}"

    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["TLSv1.2"]
    }
  }

  default_cache_behavior {
    allowed_methods        = ["GET", "HEAD", "OPTIONS"]
    cached_methods         = ["GET", "HEAD"]
    target_origin_id       = "apigw-${aws_apigatewayv2_api.public.id}"
    viewer_protocol_policy = "redirect-to-https"
    compress               = true
    cache_policy_id        = aws_cloudfront_cache_policy.api.id
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  # The default *.cloudfront.net certificate. There is no custom domain in the
  # architecture — Route 53 and ACM are not drawn, and the web tool that would
  # need one is out of scope.
  viewer_certificate {
    cloudfront_default_certificate = true
  }

  tags = {
    step = "serving"
  }
}
