#!/bin/bash

# Install AWS CLI
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install

# Configure AWS credential
aws configure set aws_access_key_id "$Access_Key"
aws configure set aws_secret_access_key "$Secret_Access_Key"
aws configure set default.region "us-west-1"  # Replace with your desired region

# Install Terraform
curl "https://releases.hashicorp.com/terraform/1.0.0/terraform_1.0.0_linux_amd64.zip" -o "terraform.zip"
unzip terraform.zip
sudo mv terraform /usr/local/bin/
