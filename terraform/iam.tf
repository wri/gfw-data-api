resource "aws_iam_policy" "run_batch_jobs" {
  name   = "${local.project}-run_batch_jobs${local.name_suffix}"
  policy = data.template_file.task_batch_policy.rendered

}

resource "aws_iam_policy" "s3_read_only" {
  name   = "${local.project}-s3_read_only${local.name_suffix}"
  policy = data.local_file.iam_s3_read_only.content
}

resource "aws_iam_policy" "lambda_invoke" {
  name   = "${local.project}-lambda_invoke${local.name_suffix}"
//  policy = data.template_file.iam_lambda_invoke.rendered
  policy = data.local_file.iam_lambda_invoke.content
}