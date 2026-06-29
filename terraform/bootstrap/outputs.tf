output "bucket_name" {
  value = aws_s3_bucket.tfstate.bucket
}

output "dynamodb_table_name" {
  value = aws_dynamodb_table.tfstate_lock.name
}

output "backend_config_hint" {
  value = <<-EOT
    Add this backend block to terraform/confluent/versions.tf:

    backend "s3" {
      bucket         = "${aws_s3_bucket.tfstate.bucket}"
      key            = "confluent/terraform.tfstate"
      region         = "${var.aws_region}"
      dynamodb_table = "${aws_dynamodb_table.tfstate_lock.name}"
      encrypt        = true
    }
  EOT
}
