output "instance_id" {
  description = "ID of the EC2 instance."
  value       = aws_instance.this.id
}

output "private_ip" {
  description = "Private IPv4 address of the EC2 instance."
  value       = aws_instance.this.private_ip
}

output "availability_zone" {
  description = "Availability Zone in which the instance runs."
  value       = aws_instance.this.availability_zone
}
