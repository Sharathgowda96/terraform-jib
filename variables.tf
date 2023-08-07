#variable "username" {
#  type    = list(any)
#  default = ["terraform-jib", ]
#}

variable "aws_region" {
  default = "us-west-1"
}

variable "s3_bucket_name" {
  type    = string
  default = "s3_bucketterra"
}

variable "aws_iam_policy_attachment" {
  type    = string
  default = "s3_full_accessattachment"
}

variable "aws_iam_policy" {
  type    = string
  default = "s3_fullaccess"
}

variable "aws_iam_role" {
  type    = string
  default = "S3_Full_AccessRole"
}
