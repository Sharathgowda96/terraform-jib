variable "username" {
  type    = list(any)
  default = ["terraform-jib", ]
}

variable "aws_region" {
  default = "us-west-1"
}

variable "instance_profile_name" {
  type    = string
  default = "example-instance-profile"
}

variable "iam_policy_name" {
  type    = string
  default = "example-policy"
}

variable "role_name" {
  type    = string
  default = "example-role"
}
