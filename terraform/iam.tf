resource "aws_iam_policy" "run_batch_jobs" {
  name   = "${local.project}-ecs_run_batch_jobs${local.name_suffix}"
  policy = data.template_file.task_batch_policy.rendered

}

resource "aws_iam_policy" "s3_read_only" {
  name   = "${local.project}-s3_write_data-lake"
  policy = data.local_file.iam_s3_read_only.content

}