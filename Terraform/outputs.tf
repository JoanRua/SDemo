output "s3_bucket_arns" {
  description = "Map of environment to S3 bucket ARNs"
  value       = module.s3.s3_bucket_arns
}
