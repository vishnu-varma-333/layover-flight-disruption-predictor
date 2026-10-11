variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Short name used in resource names and tags."
  type        = string
  default     = "layover"
}

variable "environment" {
  description = "Deployment environment tag (demo, staging, prod)."
  type        = string
  default     = "demo"
}

# The spec's own "Cost note" (DECISIONS.md, Milestone 10): one modest VM
# for the demo, not a fleet -- Kafka, TimescaleDB, Redis, Temporal and
# every app service run as containers on this single host. t3.large
# (2 vCPU, 8 GiB) is the smallest size that comfortably ran the full
# stack locally during Milestone 1-9 development; t3.xlarge is a
# reasonable step up if the demo also needs to sustain Milestone 10's
# load-test replay rate.
variable "instance_type" {
  description = "EC2 instance type for the single application host."
  type        = string
  default     = "t3.large"
}

variable "key_name" {
  description = "Existing EC2 key pair name for SSH access. Leave null to disable SSH entirely (the demo doesn't require it -- the app is reached over HTTP)."
  type        = string
  default     = null
}

variable "allowed_ssh_cidr" {
  description = "CIDR allowed to reach port 22, if key_name is set. Never leave this as 0.0.0.0/0 -- scope it to your own IP (e.g. \"203.0.113.4/32\")."
  type        = string
  default     = null

  validation {
    condition     = var.allowed_ssh_cidr == null || var.allowed_ssh_cidr != "0.0.0.0/0"
    error_message = "allowed_ssh_cidr must not be 0.0.0.0/0 -- scope SSH access to a specific address."
  }
}

variable "web_port" {
  description = "Public port the Next.js web app listens on."
  type        = number
  default     = 3001
}

variable "github_repo_url" {
  description = "Repo the instance clones at boot to run `docker compose up`."
  type        = string
  default     = "https://github.com/vishnu-varma-333/layover-flight-disruption-predictor.git"
}
