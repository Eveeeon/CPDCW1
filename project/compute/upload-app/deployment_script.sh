#!/bin/bash
# Script to run on EC2 instance to deploy app

# Update and install
yum update -y
yum install -y python3 git
python3 -m pip install boto3

# Set bucket name as environment variable to, will provide actuall name during deployment
export S3_BUCKET_NAME="{{S3_BUCKET_NAME}}"

# Clone repo
cd /home/ec2-user
git clone https://github.com/Eveeeon/CPDCW1.git

cd /home/ec2-user/CPDCW1/compute/upload-app

# create upload directory inside app
mkdir -p uploadfiles

# run uploader
python3 app.py &