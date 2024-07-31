# Create SQS Queue for Snowpipe
resource "aws_sqs_queue" "snowpipe_queue" {
  name                       = "snowpipe-queue"
  visibility_timeout_seconds = 900  
}

resource "aws_sqs_queue_policy" "snowpipe_queue_policy" {
  queue_url = aws_sqs_queue.snowpipe_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Id = "AllowSNS"
    Statement = [
      {
        Sid = "AllowSNS"
        Effect = "Allow"
        Principal = "*"
        Action = "sqs:SendMessage"
        Resource = aws_sqs_queue.snowpipe_queue.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn": aws_sns_topic.snowpipe_topic.arn
          }
        }
      }
    ]
  })
}
