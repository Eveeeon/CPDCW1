import pytest
import boto3
import os
import yaml
from moto import mock_aws
from pathlib import Path
from unittest.mock import patch
from project.orchestration import (
    add_to_deployed_resources,
    deploy_ec2,
    deploy_dynamodb,
    deploy_cloudformation,
    deploy_sns,
    run_deployment
)


@pytest.fixture(autouse=True)
def aws_credentials():
    # Set creds for moto
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"

# CONFIG
@pytest.fixture
def config():
    config_path = Path(__file__).parent / "resources.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    config["runtime"] = {}
    return config


@pytest.fixture
def base_dir():
    return Path(__file__).resolve().parent.parent


# HELPER FUNCTIONS --------------------------------------

def test_add_to_deployed_resources_new_key():
    # --- adding a resource type that doesn't exist yet should create the key
    result = add_to_deployed_resources({}, "ec2", "i-1234")
    assert result == {"ec2": ["i-1234"]}


def test_add_to_deployed_resources_existing_key():
    # --- adding to an existing resource type should append not overwrite
    result = add_to_deployed_resources({"ec2": ["i-1234"]}, "ec2", "i-5678")
    assert result == {"ec2": ["i-1234", "i-5678"]}


# DEPLOY EC2 --------------------------------------

@mock_aws
def test_deploy_ec2(config, base_dir):
    # --- create instance profile
    iam = boto3.client("iam", region_name="us-east-1")
    iam.create_instance_profile(InstanceProfileName="LabUser")
    # --- set up a fake ami to launch against
    ec2 = boto3.client("ec2", region_name="us-east-1")
    fake_ami = ec2.describe_images(Owners=["amazon"])["Images"][0]["ImageId"]
    config["resources"]["ec2"]["upload-image"]["ami-name"] = fake_ami

    # --- fake ami id for the ami name
    with patch("project.orchestration.ec2_helpers.get_ami_id", return_value=fake_ami):
        updated_config, updated_deployed = deploy_ec2(config, {}, base_dir)

    # --- check 1 - added to deployed resources
    assert "ec2" in updated_deployed
    assert len(updated_deployed["ec2"]) == 1

    # --- check 2 - it's actually deployed
    instance_id = updated_deployed["ec2"][0]
    response = ec2.describe_instances(InstanceIds=[instance_id])
    assert len(response["Reservations"]) == 1


# DEPLOY DYNAMODB --------------------------------------

@mock_aws
def test_deploy_dynamodb(config):
    updated_config, updated_deployed = deploy_dynamodb(config, {})

    # --- check 1 - added to deployed resources
    assert "dynamodb-images-s2264323" in updated_deployed["dynamodb"]

    # --- check 2 - added stream arn to config runtime
    assert "db_stream_arn" in updated_config["runtime"]
    assert updated_config["runtime"]["db_stream_arn"] is not None

    # --- check 3 - it's actually deployed
    dynamodb = boto3.client("dynamodb", region_name="us-east-1")
    response = dynamodb.describe_table(TableName="dynamodb-images-s2264323")
    assert response["Table"]["TableStatus"] == "ACTIVE"
    assert response["Table"]["StreamSpecification"]["StreamEnabled"] is True


# DEPLOY CLOUDFORMATION S3 SQS --------------------------------------

@mock_aws
def test_deploy_cloudformation(config, base_dir):
    updated_config, updated_deployed = deploy_cloudformation(config, {}, base_dir)

    # --- check 1 - added to deployed resources
    assert "stack-images-s2264323" in updated_deployed["cloudformation"]

    # --- check 2 - added queue arn to config runtime
    assert "sqs_queue_arn" in updated_config["runtime"]
    assert updated_config["runtime"]["sqs_queue_arn"] is not None

    # --- check 3 - s3 actually deployed
    s3 = boto3.client("s3", region_name="us-east-1")
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    assert "bucket-images-s2264323" in buckets

    # --- check 4 - sqs actually deployed
    sqs = boto3.client("sqs", region_name="us-east-1")
    queues = sqs.list_queues(QueueNamePrefix="queue-images-s2264323")
    assert len(queues.get("QueueUrls", [])) == 1


# DEPLOY SNS --------------------------------------

@mock_aws
def test_deploy_sns(config):
    updated_config, updated_deployed = deploy_sns(config, {})

    # --- check 1 - added topic arn to config runtime
    assert updated_config["runtime"]["sns_topic_arn"] in updated_deployed["sns"]

    # --- check 4 - sns actually deployed
    sns = boto3.client("sns", region_name="us-east-1")
    topics = sns.list_topics()["Topics"]
    topic_arns = [t["TopicArn"] for t in topics]
    assert updated_config["runtime"]["sns_topic_arn"] in topic_arns


# DEPLOY E2E --------------------------------------

@mock_aws
def test_deploy_all(config):
    # --- create role
    iam = boto3.client("iam", region_name="us-east-1")
    iam.create_role(
        RoleName="LabRole",
        AssumeRolePolicyDocument="""{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }"""
    )
    # --- create instance profile
    iam.create_instance_profile(InstanceProfileName="LabUser")
    deployed_resources = {}
    project_dir = Path(__file__).resolve().parent.parent
    deployed_resources = run_deployment(config, deployed_resources, project_dir)