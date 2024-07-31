
resource "aws_iam_role" "s3_integration" {
  for_each = var.environments

  name = "${each.key}-s3-integration"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"  # Allow S3 service to assume
        }
        Condition = {
          StringEquals = {
            "aws:SourceAccount" : data.aws_caller_identity.current.account_id
          }
        }
      }
    ]
  })
}

# Get the current AWS account ID
data "aws_caller_identity" "current" {}


resource "aws_iam_policy" "s3_full_access" {
  for_each = var.environments
  
  name        = "${each.key}-s3-full-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:*",
        ]
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.environment_bucket[each.key].arn,
          "${aws_s3_bucket.environment_bucket[each.key].arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "s3_full_access_attachment" {
  for_each = var.environments
  role       = aws_iam_role.s3_integration[each.key].name
  policy_arn = aws_iam_policy.s3_full_access[each.key].arn
}
