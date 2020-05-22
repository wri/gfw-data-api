## Requirements

| Name | Version |
|------|---------|
| terraform | >=0.12.24 |
| aws | ~> 2.56.0 |

## Providers

| Name | Version |
|------|---------|
| aws | ~> 2.56.0 |
| template | n/a |
| terraform | n/a |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| auto\_scaling\_cooldown | n/a | `number` | `300` | no |
| auto\_scaling\_max\_capacity | n/a | `number` | `15` | no |
| auto\_scaling\_max\_cpu\_util | n/a | `number` | `75` | no |
| auto\_scaling\_min\_capacity | n/a | `number` | `1` | no |
| container\_name | n/a | `string` | `"gfw-data-api"` | no |
| container\_port | n/a | `number` | `80` | no |
| desired\_count | n/a | `number` | `1` | no |
| environment | An environment namespace for the infrastructure. | `string` | n/a | yes |
| fargate\_cpu | n/a | `number` | `256` | no |
| fargate\_memory | n/a | `number` | `2048` | no |
| key\_pair | n/a | `string` | `"tmaschler_gfw"` | no |
| listener\_port | n/a | `number` | `80` | no |
| log\_level | n/a | `any` | n/a | yes |
| log\_retention | n/a | `number` | `30` | no |
| region | n/a | `string` | `"us-east-1"` | no |

## Outputs

| Name | Description |
|------|-------------|
| loadbalancer\_dns | n/a |

