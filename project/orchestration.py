import boto3
from pathlib import Path
import yaml
from typing import Dict, List
from project.resource_deployment import (
    ec2_helpers,
    lambda_helpers,
    dynamodb_helpers,
    sns_helpers,
    cloudformation_helpers,
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
        deployed_resources[resource_type].append(id)
        return deployed_resources


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
    # --- not used as a source of truth for resources, config used for that
    # --- purpose is to track deployments and terminations
    # --- keeps only enough information to terminate
    deployed_resources = {}

    # DEPLOY EC2 ---------------------------------------------------
    # --- create clients
    ec2_client = boto3.client("ec2")
    ssm_client = boto3.client("ssm")

    # --- get config details
    ec2_config = resources.get("ec2").get("upload-image")
    ami_id = ec2_helpers.get_ami_id(ssm_client, ec2_config.get("ami-name"))

    # --- deployment script to inject which will deploy the application
    app_deployment_script_path = str(
        base_dir / "compute" / "upload-app" / "deployment_script.sh"
    )
    with open(app_deployment_script_path, "r") as f:
        deployment_script = f.read()
    # Provide bucket name to be set as the environment variable
    deployment_script = deployment_script.replace(
        "{{S3_BUCKET_NAME}}", resources.get("s3").get("image-bucket").get("name")
    )

    ec2_instance_id = ec2_helpers.ec2_create(
        ec2_client,
        ami_id,
        ec2_config.get("instance-profile"),
        ec2_config.get("instance-type"),
        ec2_config.get("min-count"),
        ec2_config.get("max-count"),
        tags=deployment_tags,
        user_data_script=deployment_script,
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

    dynamodb_helpers.dynamodb_create_new_item_stream(
        dynamodb_client, dynamodb_config.get("name")
    )

    config["runtime"]["db_stream_arn"] = dynamodb_helpers.dynamodb_get_stream_arn(
        dynamodb_client, dynamodb_config.get("name")
    )

    # DEPLOY S3 AND SQS ---------------------------------------------------
    # --- create clients
    cloudformation_client = boto3.client("cloudformation")
    # --- get config details
    cloudformation_config = resources.get("cloudformation").get("image-bucket-queue")
    stack_name = cloudformation_config.get("name")
    template_path = base_dir / "cloudformation" / "s3-sqs.yaml"
    parameters = {
        "BucketName": resources.get("s3").get("image-bucket").get("name"),
        "QueueName": resources.get("sqs").get("notify-image").get("name"),
    }

    cloudformation_helpers.cloudformation_create_stack(
        cloudformation_client,
        stack_name,
        str(template_path),
        parameters,
        tags=deployment_tags,
    )

    # --- get queue arn for later for the lambda function
    cloudformation_outputs = cloudformation_helpers.cloudformation_get_outputs(
        cloudformation_client, stack_name
    )
    config["runtime"]["sqs_queue_arn"] = cloudformation_outputs["QueueArn"]

    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "cloudformation", stack_name
    )

    # CREATE TOPIC ---------------------------------------------------
    # --- create clients
    sns_client = boto3.client("sns")
    # --- get config details
    sns_config = resources.get("sns").get("email-topic")
    topic_arn = sns_helpers.sns_create_topic(
        sns_client, sns_config.get("name"), deployment_tags
    )
    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(deployed_resources, "sns", topic_arn)
    # --- add to config for using later
    config["runtime"]["sns_topic_arn"] = topic_arn

    # DEPLOY LAMBDAS ---------------------------------------------------
    # --- create clients
    lambda_client = boto3.client("lambda")
    # --- get config details
    lambda_config = resources.get("lambda")

    # --- image detection --------------------------
    lambda_img_config = lambda_config.get("image-detection")
    lambda_img_path = base_dir / "compute" / "lambda" / "image-detection"
    lambda_img_zip = lambda_helpers.lambda_zip(lambda_img_path)
    lambda_img_vars = {"DB_TABLE_NAME": dynamodb_config.get("name")}
    lambda_helpers.lambda_create(
        lambda_client,
        lambda_img_zip,
        lambda_img_config.get("name"),
        lambda_img_config.get("role"),
        lambda_img_config.get("runtime"),
        lambda_img_config.get("handler"),
        tags=deployment_tags,
        environment_variables=lambda_img_vars,
    )

    # --- create trigger
    lambda_helpers.lambda_create_sqs_trigger(
        lambda_client,
        config.get("runtime").get("sqs_queue_arn"),
        lambda_img_config.get("name"),
    )

    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "lambda", lambda_img_config.get("name")
    )

    # --- send email --------------------------
    lambda_email_config = lambda_config.get("send-email")
    lambda_email_path = base_dir / "compute" / "lambda" / "send-email"
    lambda_email_zip = lambda_helpers.lambda_zip(lambda_email_path)
    lambda_email_vars = {"SNS_TOPIC_ARN": config.get("runtime").get("sns_topic_arn")}
    lambda_helpers.lambda_create(
        lambda_client,
        lambda_email_zip,
        lambda_email_config.get("name"),
        lambda_email_config.get("role"),
        lambda_email_config.get("runtime"),
        lambda_email_config.get("handler"),
        tags=deployment_tags,
        environment_variables=lambda_email_vars,
    )

    lambda_helpers.lambda_create_dbstream_trigger(
        lambda_client,
        config.get("runtime").get("db_stream_arn"),
        lambda_email_config.get("name"),
    )

    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "lambda", lambda_email_config.get("name")
    )
