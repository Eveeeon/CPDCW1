#!/bin/bash
# Script to run on EC2 instance to deploy app

# Update and install
yum update -y
yum install -y python3 git python3-pip
python3 -m pip install boto3

# Set bucket name as environment variable to, will provide actual name during deployment
export S3_BUCKET_NAME="{{S3_BUCKET_NAME}}"

# Clone repo
cd /home/ec2-user
git clone https://github.com/Eveeeon/CPDCW1.git

# Create upload directory in home
mkdir -p /home/ec2-user/CPDCW1/project/compute/upload-app/uploadfiles
# Make sure it is writable
chown ec2-user:ec2-user /home/ec2-user/CPDCW1/project/compute/upload-app/uploadfiles

# Run app
cd /home/ec2-user/CPDCW1/project/compute/upload-app
python3 app.py &
