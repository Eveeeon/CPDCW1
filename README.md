# CPDCW1 s2264323
An automated deployment project for an image detection application hosted on AWS.

The application detects driving and facial emotions in pictures, and sends email alerts if anger and driving are detected.

The application is modular and extensible beyond the initial scope.

# Prerequisites

- Python 3.x
- AWS CLI configured with credentials
- Access to an AWS environment with Rekognition, Lambda, S3, SQS, DynamoDB, SNS, and EC2

---

## Setup

1. Clone this repository
   ```bash
   git clone https://github.com/Eveeeon/CPDCW1.git
   ```

2. Configure the resources configuration file
   Edit `project/config/resources.yaml` with your resource names, email, tags, IAM roles, instance profile, and default security group ID.

3. Set AWS credentials
   Copy credentials to:
   - Windows: `C:\Users\{user}\.aws\credentials`
   - Linux: `~/.aws/credentials`  
   
   Ensure the region is specified.

4. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

---

## Deployment

```bash
python main.py
```

Press **Enter** in the terminal when done to automatically terminate and delete all deployed resources.

---

## Testing

```bash
pytest project/test/test.py
```

---

## Uploading Images to EC2

_Ensure the SNS email subscription has been confirmed before uploading._

1. Generate a local key pair
   ```bash
   ssh-keygen -t rsa -b 4096 -f key.pem
   ```

2. Get the public key
   ```bash
   cat key.pem.pub
   ```

3. Connect to the EC2 instance via the AWS Console and add the public key to authorised keys
   ```bash
   echo "{public key}" >> ~/.ssh/authorized_keys
   chmod 600 ~/.ssh/authorized_keys
   ```

4. Get the EC2 public IPv4 from the AWS Console

5. Upload images via SCP
   ```bash
   scp -i key.pem /path/to/images/* ec2-user@{EC2 IP}:/home/ec2-user/CPDCW1/project/compute/upload-app/uploadfiles
   ```
