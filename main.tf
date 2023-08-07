provider "aws" {
  access_key = var.access_key
  secret_key = var.secret_key
  region = var.aws_region
}

terraform {
  backend "s3" {
    bucket = "testing-s3-with-terra"
    key    = "*"
    region = "us-east-1"
  }
}


resource "aws_s3_bucket" "onebucket" {
  bucket = var.s3_bucket_name
  acl    = "private"
  versioning {
    enabled = true
  }
  tags = {
    Name        = "Bucket1"
    Environment = "Test"
  }
}

resource "aws_iam_policy" "s3_full_access" {
  name = var.aws_iam_policy

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::testing-s3-with-terra"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::testing-s3-with-terra/*"
    }
  ]
}
}

resource "aws_iam_policy" "s3_full_access" {
  name = var.aws_iam_policy

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::testing-s3-with-terra",
        "arn:aws:s3:::testing-s3-with-terra/*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role" "s3_role" {
  name = var.aws_iam_role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "s3_full_access_attachment" {
  name       = var.aws_iam_policy_attachment
  policy_arn = aws_iam_policy.s3_full_access.arn
  roles      = [aws_iam_role.s3_role.name]
}
