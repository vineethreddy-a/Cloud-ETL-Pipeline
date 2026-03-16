# ─── Terraform: AWS ETL Infrastructure ───────────────────────────────────────
# Provisions: S3 buckets, Glue, EMR, Redshift, IAM roles, SNS alerting

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket = "my-terraform-state-bucket"
    key    = "etl-pipeline/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# ─── Variables ────────────────────────────────────────────────────────────────

variable "aws_region"   { default = "us-east-1" }
variable "environment"  { default = "dev" }
variable "project_name" { default = "etl-pipeline" }

locals {
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Owner       = "vineeth.aredla"
  }
}

# ─── S3 Buckets ───────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data-lake-${var.environment}"
  tags   = local.tags
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ─── IAM Role for EMR ─────────────────────────────────────────────────────────

resource "aws_iam_role" "emr_role" {
  name = "${var.project_name}-emr-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "elasticmapreduce.amazonaws.com" }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "emr_s3_access" {
  role       = aws_iam_role.emr_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# ─── AWS Glue Database ────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "etl_db" {
  name = "${var.project_name}_${var.environment}"
}

# ─── SNS Alert Topic ──────────────────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-alerts-${var.environment}"
  tags = local.tags
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = "aredlavineethreddy2001@gmail.com"
}

# ─── Outputs ──────────────────────────────────────────────────────────────────

output "data_lake_bucket" {
  value = aws_s3_bucket.data_lake.bucket
}

output "sns_topic_arn" {
  value = aws_sns_topic.pipeline_alerts.arn
}
