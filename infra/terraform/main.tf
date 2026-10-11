locals {
  name = "${var.project}-${var.environment}"
}

# --- Network: the account's existing default VPC, not a new one -------
# A dedicated VPC is the right call at a scale where network isolation
# actually matters; for one demo host, provisioning a VPC, subnets,
# route tables, an internet gateway and NAT just to end up with the
# same public-reachability this already gives is complexity the spec's
# own cost-conscious framing doesn't ask for. See DECISIONS.md.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# --- Security group -----------------------------------------------------
resource "aws_security_group" "app" {
  name        = "${local.name}-app"
  description = "Layover demo host: web app inbound, SSH only if a key was supplied"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Next.js web app"
    from_port   = var.web_port
    to_port     = var.web_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  dynamic "ingress" {
    for_each = var.key_name != null && var.allowed_ssh_cidr != null ? [1] : []
    content {
      description = "SSH, scoped to allowed_ssh_cidr"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = [var.allowed_ssh_cidr]
    }
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-app" }
}

# --- S3: processed data + MLflow model artifacts -----------------------
# What the Dockerfile's own comment describes as the real production
# path (containers pull the active model + flights.parquet from here at
# startup, instead of the demo's COPY-into-the-image shortcut).
resource "aws_s3_bucket" "data" {
  bucket = "${local.name}-data-${data.aws_caller_identity.current.account_id}"
  tags   = { Name = "${local.name}-data" }
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# --- Secrets Manager -----------------------------------------------------
# Real, random secrets at plan/apply time -- never a placeholder string
# committed to the repo or baked into the AMI. The instance reads these
# at boot (see user_data.sh.tftpl) rather than having them passed in
# plaintext user_data, which AWS does not encrypt at rest.
resource "random_password" "session_secret" {
  length  = 48
  special = false
}

resource "random_password" "db_password" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "session_secret" {
  name = "${local.name}/session-secret"
}

resource "aws_secretsmanager_secret_version" "session_secret" {
  secret_id     = aws_secretsmanager_secret.session_secret.id
  secret_string = random_password.session_secret.result
}

resource "aws_secretsmanager_secret" "db_password" {
  name = "${local.name}/db-password"
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id     = aws_secretsmanager_secret.db_password.id
  secret_string = random_password.db_password.result
}

# --- IAM: least-privilege instance role ----------------------------------
# Scoped to exactly this bucket and these two secrets -- not
# AmazonS3FullAccess / SecretsManagerReadWrite, and no write access to
# secrets at all (the instance only ever reads them).
data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "app" {
  name               = "${local.name}-app"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

data "aws_iam_policy_document" "app" {
  statement {
    sid       = "ReadWriteDataBucket"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
  }
  statement {
    sid       = "ReadOwnSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.session_secret.arn, aws_secretsmanager_secret.db_password.arn]
  }
}

resource "aws_iam_role_policy" "app" {
  name   = "${local.name}-app"
  role   = aws_iam_role.app.id
  policy = data.aws_iam_policy_document.app.json
}

resource "aws_iam_instance_profile" "app" {
  name = "${local.name}-app"
  role = aws_iam_role.app.name
}

# --- The single application host ----------------------------------------
resource "aws_instance" "app" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.app.id]
  iam_instance_profile   = aws_iam_instance_profile.app.name
  key_name               = var.key_name

  root_block_device {
    volume_size = 40
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    github_repo_url    = var.github_repo_url
    data_bucket        = aws_s3_bucket.data.bucket
    session_secret_arn = aws_secretsmanager_secret.session_secret.arn
    db_password_arn    = aws_secretsmanager_secret.db_password.arn
    aws_region         = var.aws_region
  })

  tags = { Name = local.name }
}
