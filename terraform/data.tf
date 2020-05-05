# import core state
data "terraform_remote_state" "core" {
  backend = "s3"
  config = {
    bucket = local.tf_state_bucket
    region = "us-east-1"
    key    = "core.tfstate"
  }
}


data "template_file" "container_definition" {
  template = file("${path.root}/templates/container_definition.json.tmpl")
  vars = {
    image = "${module.container_registry.repository_url}:latest"

    container_name = var.container_name
    container_port = var.container_port

    log_group = aws_cloudwatch_log_group.default.name

   reader_secret_arn =  data.terraform_remote_state.core.outputs.secrets_postgresql-reader_arn
    writer_secret_arn  =  data.terraform_remote_state.core.outputs.secrets_postgresql-writer_arn
    log_level   = var.log_level
    project     = local.project
    environment = var.environment
    aws_region  = var.region
  }
}