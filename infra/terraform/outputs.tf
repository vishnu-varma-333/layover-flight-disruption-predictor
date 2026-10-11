output "instance_public_ip" {
  description = "Public IP of the application host. The web app is reachable at http://<this>:<web_port>."
  value       = aws_instance.app.public_ip
}

output "instance_id" {
  value = aws_instance.app.id
}

output "data_bucket_name" {
  value = aws_s3_bucket.data.bucket
}

output "session_secret_arn" {
  value = aws_secretsmanager_secret.session_secret.arn
}

output "db_password_arn" {
  value = aws_secretsmanager_secret.db_password.arn
}
