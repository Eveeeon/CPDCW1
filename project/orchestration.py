import boto3
from pathlib import Path
import yaml
from typing import Dict, List
from project.resource_deployment import (
    ec2_helpers,
    lambda_helpers,
    dynamodb_helpers,
    sns_helpers,
)


def add_to_deployed_resources(
    deployed_resources: Dict[List], resource_type: str, id: str
) -> Dict[List]:
    """
    Helper to add resources to a deployed resources dictionary
    The deployed resources dictionary contains a list of resource ids for each service
    Will add the service if it doesn't already exist, otherwise it will append the id

    Args:
        deployed_resources (Dict[List]): the dictionary tracking the deployed resources
        resource_type (str): the type of resources e.g. lambda
        id (str): the identifier of the resources

    Returns:
        Dict[List]: the updated deployed-resources dictionary
    """
    if resource_type not in deployed_resources:
        deployed_resources[resource_type] = [id]
        return deployed_resources
    else:
        deployed_resources[resource_type] = deployed_resources[resource_type].append(id)


def run():
    base_dir = Path(__file__).resolve().parent

    # CONFIG
    config_path = base_dir / "config" / "resources.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    resources = config.get("resources")

    # TAGS FOR APPLICATION
    deployment_tags = config.get("tags")

    # TRACK EVERYTHING THAT IS DEPLOYED
    deployed_resources = {}

    # DEPLOY EC2 ---------------------------------------------------
    # --- create clients
    ec2_client = boto3.client("ec2")
    smm_client = boto3.client("smm")

    # --- get config details
    ec2_config = resources.get("ec2").get("upload-image")
    ami_id = ec2_helpers.get_ami_id(smm_client, ec2_config.get("ami-name"))

    # --- deployment script to inject which will deploy the application
    app_deployment_script = base_dir / "compute" / "upload-app" / "deployment_script.sh"

    ec2_instance_id = ec2_helpers.ec2_create(
        ec2_client,
        ami_id,
        ec2_config.get("instance-profile"),
        ec2_config.get("min-count"),
        ec2_config.get("max-count"),
        tags=deployment_tags,
        user_data_script_path=str(app_deployment_script),
    )
    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "ec2", ec2_instance_id
    )

    # DEPLOY DYNAMODB ---------------------------------------------------
    # --- create clients
    dynamodb_client = boto3.client("dynamodb")
    # --- get config details
    dynamodb_config = resources.get("dynamodb").get("images-db")

    dynamodb_helpers.dynamodb_create_table(
        dynamodb_client,
        dynamodb_config.get("name"),
        dynamodb_config.get("partition-key"),
        dynamodb_config.get("partition-key-type"),
        deployment_tags,
    )

    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "dynamodb", dynamodb_config.get("name")
    )

    # DEPLOY S3 AND SQS ---------------------------------------------------

    # CREATE TOPIC ---------------------------------------------------
    # --- create clients
    sns_client = boto3.client("sns")
    # --- get config details
    sns_config = resources.get("sns").get("email-topic")
    topic_arn = sns_helpers.sns_create_topic(
        sns_client, sns_config.get("name"), deployment_tags
    )
    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "sns", topic_arn
    )

    # DEPLOY LAMBDAS ---------------------------------------------------
