# AWS Santex Demo - Terraform Project

## Overview

This Terraform project automates the creation and configuration of:

- **S3 Buckets:** One bucket per environment (RAW, STAGE, PROD).
- **IAM Roles:** Specific roles for S3 integration with appropriate permissions.
- **SQS Queue:** A queue to receive Snowpipe event notifications.
- **SNS Topic:** A topic to publish S3 events for Snowpipe.
- **Snowpipe Event Notifications:** Triggers Snowpipe to load data from S3.

## Project Structure

├── main.tf          # Core infrastructure code
├── variables.tf     # Input variables
├── outputs.tf       # Resources to expose after creation
├── iam.tf           # IAM roles and policies
├── s3.tf            # S3 bucket creation and configuration
├── sqs.tf           # SQS queue configuration
└── README.md        # This documentation file

## Prerequisites

- **Terraform:** Installed and configured. (See: [https://learn.hashicorp.com/tutorials/terraform/install-cli](https://learn.hashicorp.com/tutorials/terraform/install-cli))   

- **AWS Account:** With appropriate permissions to create S3 buckets, IAM roles, SQS queues, and SNS topics.
- **Snowflake Account:** With a configured Snowpipe pipeline and the ARN of your SQS queue.

## Configuration

1. **Variables:**
   - Open `variables.tf` and customize the following:
     - `aws_region`: The AWS region to deploy your resources.
     - `environments`: List of environments (RAW, STAGE, PROD).

## Deployment

1. **Initialize:**
   ```bash
   terraform init

2. **Plam:**
   ```bash
   terraform plan

3. **Plam:**
   ```bash
   terraform apply
