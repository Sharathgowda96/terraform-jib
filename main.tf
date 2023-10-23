provider "aws" {
  access_key = var.access_key
  secret_key = var.secret_key
  region = var.aws_region
}

terraform {
  backend "s3" {
    bucket = "testing-s3-with-terra"
    key    = "terraform.tfstate"
    region = "us-east-1"
  }
}

resource "aws_lambda_function" "jib-lambda" {
  function_name = "sample-lambda"
  handler      = "index.handler"
  runtime      = "nodejs14.x"
  role         = aws_iam_role.s3_role.arn

  source_code_hash = filebase64sha256("${path.module}/lambda.zip")
}

resource "aws_glue_job" "jib_glue_job" {
  name    = "glue-job"
  role_arn = aws_iam_role.s3_role.arn
  command {
    name           = "glueetl"
    script_location = "s3://your-script-location"
  }
  default_arguments = {
    "--job-language" = "python"
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
