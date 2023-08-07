#variable "username" {
#  type    = list(any)
#  default = ["terraform-jib", ]
#}

variable "aws_region" {
  default = "us-east-1"
}

variable "access_key" {
  default = "AKIAWIPSST26DLHVBEOF"
}

variable "secret_key" {
  default = "ijI8vXR9/EFtVXnfvlxPJuGkRDLlfQp0rSiGDB9a"
}

variable "s3_bucket_name" {
  type    = string
  default = "testing-s3-with-terra"
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
