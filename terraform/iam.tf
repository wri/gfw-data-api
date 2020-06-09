resource "aws_iam_policy" "s3_write_data-lake" {
  name   = "${local.project}-ecs_run_batch_jobs${local.name_suffix}"
  policy = data.template_file.task_batch_policy.rendered

}
