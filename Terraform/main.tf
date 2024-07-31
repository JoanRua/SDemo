terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"  # Adapt version if needed
    }
  }
}

provider "aws" {
  region = var.aws_region
}

module "iam" {
  source = "./iam"
  environments = var.environments
}

module "sqs" {
  source = "./sqs"
}

module "s3" {
  source = "./s3"
  environments = var.environments
  iam_roles = module.iam.s3_integration_roles
  snowpipe_queue_arn = module.sqs.snowpipe_queue_arn
}
