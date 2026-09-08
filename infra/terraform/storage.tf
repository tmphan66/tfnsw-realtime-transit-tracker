terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "ap-southeast-2"
}

resource "aws_dynamodb_table" "vehicle_state" {
  name         = "transit-tracker-vehicle-state"
  billing_mode = "PAY_PER_REQUEST"  # on-demand — no cost while idle
  hash_key     = "vehicle_id"

  attribute {
    name = "vehicle_id"
    type = "S"
  }

  tags = {
    project = "transit-tracker"
  }
}

resource "aws_s3_bucket" "history" {
  bucket = "transit-tracker-history-${data.aws_caller_identity.current.account_id}"

  tags = {
    project = "transit-tracker"
  }
}

data "aws_caller_identity" "current" {}

resource "aws_s3_bucket_lifecycle_configuration" "history_lifecycle" {
  bucket = aws_s3_bucket.history.id

  rule {
    id     = "expire-old-data"
    status = "Enabled"

    filter {}

    expiration {
      days = 90  # cost-control, can be adjusted for longer
    }
  }
}