# Production EC2 with Terraform

This configuration provisions one Amazon Linux 2023 EC2 instance using an existing subnet and security group.

## Security defaults

- IMDSv2 is required.
- The root EBS volume is encrypted and uses gp3.
- Detailed monitoring is enabled.
- Termination protection is enabled by default.
- No security group rules are created; provide an existing least-privilege security group.
- Public IP assignment is disabled by default.

## Prerequisites

- Terraform 1.6 or later
- AWS credentials configured through IAM Identity Center, environment variables, or a CI role
- An existing private subnet, security group, and (optionally) least-privilege IAM instance profile

## Usage

1. Copy `terraform.tfvars.example` to `terraform.tfvars`.
2. Replace placeholder IDs and values with your AWS environment values.
3. Review the deployment, then apply it:

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan -out=tfplan
terraform apply tfplan
```

Do not commit `terraform.tfvars` or any credential files.
