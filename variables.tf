variable "username" {
  type    = list(any)
  default = ["terraform-jib", ]
}

variable "aws_region" {
  default = "us-west-1"
}

variable "s3_bucket_name" {
  type    = string
  default = "s3_bucket_terra"
}

variable "aws_iam_policy_attachment" {
  type    = string
  default = "s3_full_access_attachment"
}

variable "aws_iam_policy" {
  type    = string
  default = "s3_full_access"
}

variable "aws_iam_role " {
  type    = string
  default = "S3FullAccessRole"
}
