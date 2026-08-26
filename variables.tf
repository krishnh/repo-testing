variable "aws_region" {
  description = "AWS region in which to create the instance."
  type        = string
}

variable "name" {
  description = "Name applied to the EC2 instance and its root volume."
  type        = string
}

variable "subnet_id" {
  description = "Existing subnet ID for the instance. Use a private subnet for production workloads."
  type        = string
}

variable "security_group_ids" {
  description = "Existing security group IDs. Define only the minimum required ingress outside this module."
  type        = list(string)

  validation {
    condition     = length(var.security_group_ids) > 0
    error_message = "At least one existing security group ID is required."
  }
}

variable "instance_type" {
  description = "EC2 instance type."
  type        = string
  default     = "t3.micro"
}

variable "iam_instance_profile_name" {
  description = "Optional IAM instance profile name. Prefer a least-privilege profile for application access."
  type        = string
  default     = null
}

variable "key_name" {
  description = "Optional EC2 key pair name. Prefer AWS Systems Manager Session Manager over SSH."
  type        = string
  default     = null
}

variable "associate_public_ip_address" {
  description = "Whether the instance receives a public IPv4 address. Keep false for private production workloads."
  type        = bool
  default     = false
}

variable "enable_termination_protection" {
  description = "Protect the instance from accidental termination."
  type        = bool
  default     = true
}

variable "root_volume_size_gib" {
  description = "Size of the encrypted gp3 root volume in GiB."
  type        = number
  default     = 20

  validation {
    condition     = var.root_volume_size_gib >= 8
    error_message = "root_volume_size_gib must be at least 8 GiB."
  }
}

variable "tags" {
  description = "Tags applied to all supported AWS resources."
  type        = map(string)
  default     = {}
}
