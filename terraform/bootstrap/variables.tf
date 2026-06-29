variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "bucket_name" {
  type        = string
  description = "Name of the S3 bucket for Terraform state. Must be globally unique."
}

variable "dynamodb_table_name" {
  type    = string
  default = "kafka-claim-poc-tfstate-lock"
}
