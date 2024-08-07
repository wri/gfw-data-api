resource "aws_iam_policy" "run_batch_jobs" {
  name   = substr("${local.project}-run_batch_jobs${local.name_suffix}", 0, 64)
  policy = data.template_file.task_batch_policy.rendered
}

resource "aws_iam_policy" "query_batch_jobs" {
  name   = substr("${local.project}-query_batch_jobs${local.name_suffix}", 0, 64)
  policy = data.template_file.query_batch_task_policy.rendered
}

resource "aws_iam_policy" "s3_read_only" {
  name   = substr("${local.project}-s3_read_only${local.name_suffix}", 0, 64)
  policy = data.local_file.iam_s3_read_only.content
}

resource "aws_iam_policy" "lambda_invoke" {
  name = substr("${local.project}-lambda_invoke${local.name_suffix}", 0, 64)
  //  policy = data.template_file.iam_lambda_invoke.rendered
  policy = data.local_file.iam_lambda_invoke.content
}

resource "aws_iam_policy" "iam_api_gateway_policy" {
  name = substr("${local.project}-api_gateway${local.name_suffix}", 0, 64)
  policy = data.local_file.iam_api_gateway_policy.content
}

resource "aws_iam_policy" "read_gcs_secret" {
  name = substr("${local.project}-read_gcs_secret${local.name_suffix}", 0, 64)
  policy = data.aws_iam_policy_document.read_gcs_secret_doc.json
}

resource "aws_iam_policy" "read_new_relic_secret" {
  name = substr("${local.project}-read_new-relic_secret${local.name_suffix}", 0, 64)
  policy = data.aws_iam_policy_document.read_new_relic_lic.json
}

resource "aws_iam_policy" "tile_cache_bucket_policy" {
  name   = substr("${local.project}-tile_cache_bucket_policy${local.name_suffix}", 0, 64)
  policy = data.template_file.tile_cache_bucket_policy.rendered
}

resource "aws_iam_policy" "step_function_policy" {
  name   = substr("${local.project}-step_function_policy${local.name_suffix}", 0, 64)
  policy = data.template_file.step_function_policy.rendered
}
