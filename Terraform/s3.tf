# ... (Creates S3 buckets and configures folders/Snowpipe)

resource "aws_s3_bucket" "environment_bucket" {
  for_each = var.environments

  bucket = "${each.key}"
  
  dynamic "versioning" {
    for_each = each.key == "PROD" ? [1] : []
    content {
      enabled = true
    }
  }

}

resource "aws_s3_bucket_notification" "snowpipe_notification" {
  bucket = aws_s3_bucket.environment_bucket["RAW"].id
  # ... (Configures Snowpipe event notification)
}


# Create Folders within Buckets
resource "aws_s3_object" "snowpipe_data_folder" {
  bucket = aws_s3_bucket.environment_bucket["RAW"].id
  key    = "snowpipe_data/"
}

resource "aws_s3_object" "edw_folder" {
  bucket = aws_s3_bucket.environment_bucket["PROD"].id
  key    = "EDW/"
}

# Enable Snowpipe Event Notifications
resource "aws_s3_bucket_notification" "snowpipe_notification" {
  bucket = aws_s3_bucket.environment_bucket["RAW"].id

  topic {
    topic_arn     = aws_sns_topic.snowpipe_topic.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "snowpipe_data/"  # Filter events to the Snowpipe folder
  }
}

# Create an SNS topic for Snowpipe
resource "aws_sns_topic" "snowpipe_topic" {
  name = "snowpipe-notifications"
}

# Add Snowflake SQS Queue as Subscriber
resource "aws_sns_topic_subscription" "snowpipe_subscription" {
  topic_arn = aws_sns_topic.snowpipe_topic.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.snowpipe_queue.arn  
}
