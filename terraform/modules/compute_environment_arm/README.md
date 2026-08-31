# Batch Compute Environment (ARM64 / Graviton)

Local, arm64 counterpart of the vendored
`git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/compute_environment`
module used elsewhere in this project.

## Why this exists

The vendored module's AMI lookup is hardcoded to `architecture = "x86_64"`
(see its `data.tf`), so it can never select a Graviton-compatible AMI --
handing it arm64 instance types would just fail at launch time, since EC2
refuses to boot an x86_64 AMI on an arm64 instance type. This module is a
near-identical copy that:

- Filters for the arm64 ECS-optimized AMI (`amzn2-ami-ecs-hvm-*-arm64-ebs`)
  instead of the legacy Amazon Linux 1 x86_64 one.
- Defaults `instance_types` to Graviton NVMe-backed families (`r7gd`, `c7gd`)
  as the arm64 equivalent of the vendored module's `r5d`/`c5d` defaults.
- Otherwise behaves identically: same IAM roles, same ephemeral-storage
  mount/swap user-data script (device naming is unchanged on Nitro-based
  Graviton instances), same outputs.

This is intentionally a drop-in replacement for the vendored module at each
call site (same resource names inside: `aws_batch_compute_environment.default`,
IAM roles, etc.), so switching a `module` block's `source` from the vendored
compute_environment module to this one converts that compute environment to
ARM64 in place -- Terraform sees it as updates to the same resource
addresses, not a teardown/recreate of a differently-named module. This
project uses that to replace its x86_64 Batch compute environments with
Graviton ones directly, rather than running both side by side: AWS Batch
enforces an account-level limit on the number of compute environments, so
doubling them for a side-by-side rollout isn't free the way idle EC2
capacity is.

Once this proves out, the equivalent change could be upstreamed into
`wri/gfw-terraform-modules` itself (adding an `architecture` variable) and
this local copy retired.

## Usage

```terraform
module "batch_data_lake_writer" {
  source = "./modules/compute_environment_arm"

  project               = local.project
  key_pair              = var.key_pair
  ecs_role_policy_arns  = [...]
  security_group_ids    = [...]
  subnets               = data.terraform_remote_state.core.outputs.private_subnet_ids
  tags                  = local.tags
  suffix                = local.name_suffix
  max_vcpus             = var.data_lake_max_vcpus
  instance_types        = var.data_lake_writer_instance_types
  use_ephemeral_storage = true
  compute_environment_name = "data_lake_writer"
}
```

Existing job *definitions* (`data_lake`, `pixetl`, `tile_cache`, `aurora`)
don't need to change: they just reference an image URI/tag, and the
container runtime on the (now ARM) instance a job lands on pulls whatever
architecture that image was built for -- see
`terraform/scripts/buildx_push.sh`, which now builds those images for
`linux/arm64` by default.
