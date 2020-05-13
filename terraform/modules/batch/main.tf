locals{
  bucket_suffix = var.environment == "production" ? "" : "-${var.environment}"
}

resource "aws_batch_job_definition" "aurora" {
  name                 = "${var.project}-aurora${var.name_suffix}"
  type                 = "container"
  container_properties = data.template_file.container_properties.rendered
}

resource "aws_batch_job_queue" "aurora" {
  name                 = "${var.project}-aurora-job-queue${var.name_suffix}"
  state                = "ENABLED"
  priority             = 1
  compute_environments = [var.aurora_compute_environment_arn]
  depends_on           = [var.aurora_compute_environment_arn]
}

# TODO: Define container properties for data lake jobs
resource "aws_batch_job_definition" "data_lake" {
  name                 = "${var.project}-data-lake${var.name_suffix}"
  type                 = "container"
  container_properties = data.template_file.container_properties.rendered
}

resource "aws_batch_job_queue" "data_lake" {
  name                 = "${var.project}-data-lake-job-queue${var.name_suffix}"
  state                = "ENABLED"
  priority             = 1
  compute_environments = [var.data_lake_compute_environment_arn]
  depends_on           = [var.data_lake_compute_environment_arn]
}

# TODO: Define container properties for tile cache jobs
resource "aws_batch_job_definition" "tile_cache" {
  name                 = "${var.project}-tile_cache${var.name_suffix}"
  type                 = "container"
  container_properties = data.template_file.container_properties.rendered
}

resource "aws_batch_job_queue" "tile_cache" {
  name                 = "${var.project}-tile_cache-job-queue${var.name_suffix}"
  state                = "ENABLED"
  priority             = 1
  compute_environments = [var.tile_cache_compute_environment_arn]
  depends_on           = [var.tile_cache_compute_environment_arn]
}

data "template_file" "container_properties" {
  template = file("${path.root}/templates/container_properties.json.tmpl")
  vars = {
    image_url      = var.repository_url
    environment    = var.environment
    job_role_arn   = aws_iam_role.aws_ecs_service_role.arn
    clone_role_arn = aws_iam_role.aws_ecs_service_role_clone.arn
    cpu            = 1
    memory         = 480
    hardULimit     = 1024
    softULimit     = 1024
    tile_cache     = "gfw-tiles${local.bucket_suffix}"
    writer_secret_arn = var.writer_secret_arn
    reader_secret_arn = var.reader_secret_arn
  }
}

resource "aws_iam_role" "aws_ecs_service_role" {
  name               = substr("${var.project}-ecs_service_role${var.name_suffix}", 0, 64)
  assume_role_policy = data.template_file.ecs-task_assume.rendered
}


resource "aws_iam_role_policy_attachment" "s3_read_only" {
  role       = aws_iam_role.aws_ecs_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "s3_write_data-lake" {
  role       = aws_iam_role.aws_ecs_service_role.name
  policy_arn = var.s3_write_data-lake_arn
}

resource "aws_iam_role_policy" "test_policy" {
  name   = substr("${var.project}-ecs_service_role_assume${var.name_suffix}", 0, 64)
  role   = aws_iam_role.aws_ecs_service_role.name
  policy = data.template_file.iam_assume_role.rendered
}


## Clone role, and allow orginal to assume clone. -> Needed to get credentials for GDALWarp

resource "aws_iam_role" "aws_ecs_service_role_clone" {
  name               = substr("${var.project}-ecs_service_role_clone${var.name_suffix}", 0, 64)
  assume_role_policy = data.template_file.iam_trust_entity.rendered
}

resource "aws_iam_role_policy_attachment" "s3_read_only_clone" {
  role       = aws_iam_role.aws_ecs_service_role_clone.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "s3_write_data-lake_clone" {
  role       = aws_iam_role.aws_ecs_service_role_clone.name
  policy_arn = var.s3_write_data-lake_arn
}



data "template_file" "iam_trust_entity" {
  template = file("${path.root}/templates/iam_trust_entity.json.tmpl")
  vars = {
    role_arn = aws_iam_role.aws_ecs_service_role.arn
  }
}

data "template_file" "iam_assume_role" {
  template = file("${path.root}/templates/iam_assume_role.json.tmpl")
  vars = {
    role_arn = aws_iam_role.aws_ecs_service_role_clone.arn
  }
}

data "template_file" "ecs-task_assume" {
  template = file("${path.root}/templates/role-trust-policy.json.tmpl")
  vars = {
    service = "ecs-tasks"
  }
}
