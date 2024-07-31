variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environments" {
  description = "List of environments (RAW, STAGE, PROD)"
  type        = list(string)
  default     = ["RAW", "STAGE", "PROD"]
}

variable "bucket_prefix" {
  description = "Prefix of environments (RAW, STAGE, PROD)"
  type        = list(string)
  default     = ["RAW", "STAGE", "PROD"]
}
