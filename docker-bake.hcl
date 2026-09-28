group "test" {
  targets = [
    "batch_jobs_test",
    "pixetl_test",
    "test_database_14",
    "app_test",
  ]
}

target "batch_jobs_test" {
  context    = "."
  dockerfile = "batch/universal_batch.dockerfile"
  tags       = ["batch_jobs_test"]
}

target "pixetl_test" {
  context    = "."
  dockerfile = "batch/pixetl.dockerfile"
  tags       = ["pixetl_test"]
}

target "test_database_14" {
  context    = "./docker/postgis"
  dockerfile = "Dockerfile"
  tags       = ["gfw-data-api-postgis:latest"]
}

target "app_test" {
  context    = "."
  dockerfile = "Dockerfile"
  args = {
    ENV = "test"
  }
  tags     = ["gfw-data-api_test-app_test"]
  no-cache = true
}
