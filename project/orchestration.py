import boto3
from pathlib import Path
import yaml
from typing import Dict, List, Tuple
import logging
from project.resource_deployment import (
    ec2_helpers,
    lambda_helpers,
    dynamodb_helpers,
    sns_helpers,
    cloudformation_helpers,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("orchestration")


def run():
    base_dir = Path(__file__).resolve().parent

    # CONFIG
    config_path = base_dir / "config" / "resources.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    # --- init runtime config
    config["runtime"] = {}
    resources = config.get("resources")

    # TAGS FOR APPLICATION
    deployment_tags = config.get("tags")

    # TRACK EVERYTHING THAT IS DEPLOYED
    # --- not used as a source of truth for resources, config used for that
    # --- purpose is to track deployments and terminations
    # --- keeps only enough information to terminate
    deployed_resources = {}

    # RUN DEPLOYMENT FUNCTIONS
    try:
        deployed_resources = run_deployment(config, deployed_resources, base_dir)
        # --- allow for resources to be automatically decomissioned
        input("Press enter to delete all deployed resources...")
        cleanup(deployed_resources)
    except Exception as e:
        raise


def run_deployment(config: Dict, deployed_resources: Dict, base_dir: Path) -> Dict:
    """
    Runs all deployments, takes config, path, and previously deployed resources, adding to them
    The purpose is to separate deployment from config management for testing

    Args:
        config (Dict): config dict
        deployed_resources (Dict): the deployed resourced object to add to
        base_dir (Path): the base directory of the deployment project

    Returns:
        Dict: updated deployed resources dictionary
    """
    try:
        config, deployed_resources = deploy_ec2(config, deployed_resources, base_dir)
        config, deployed_resources = deploy_dynamodb(config, deployed_resources)
        config, deployed_resources = deploy_cloudformation(
            config, deployed_resources, base_dir
        )
        config, deployed_resources = deploy_sns(config, deployed_resources)
        config, deployed_resources = deploy_lambdas(
            config, deployed_resources, base_dir
        )
        return deployed_resources
    except Exception as e:
        raise


# CLEANUP FUNCTION ---------------------------------------------------


def cleanup(deployed_resources: Dict):
    """
    Tears down all deployed resources tracked in deployed_resources

    Args:
        deployed_resources (Dict): dictionary of deployed resource ids
    """

    for instance_id in deployed_resources.get("ec2", []):
        try:
            ec2_helpers.ec2_terminate(boto3.client("ec2"), instance_id)
            logger.info(f"Terminated EC2 instance: {instance_id}")
        except Exception as e:
            logger.error(f"Failed to terminate EC2 {instance_id}: {e}")

    for table_name in deployed_resources.get("dynamodb", []):
        try:
            dynamodb_helpers.dynamodb_delete(boto3.client("dynamodb"), table_name)
            logger.info(f"Deleted DynamoDB table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to delete DynamoDB table {table_name}: {e}")

    for stack_name in deployed_resources.get("cloudformation", []):
        try:
            cloudformation_helpers.cloudformation_delete_stack(
                boto3.client("cloudformation"), stack_name
            )
            logger.info(f"Deleted CloudFormation stack: {stack_name}")
        except Exception as e:
            logger.error(f"Failed to delete CloudFormation stack {stack_name}: {e}")

    for topic_arn in deployed_resources.get("sns", []):
        try:
            boto3.client("sns").delete_topic(TopicArn=topic_arn)
            logger.info(f"Deleted SNS topic: {topic_arn}")
        except Exception as e:
            logger.error(f"Failed to delete SNS topic {topic_arn}: {e}")

    for function_name in deployed_resources.get("lambda", []):
        try:
            lambda_helpers.lambda_delete(boto3.client("lambda"), function_name)
            logger.info(f"Deleted Lambda function: {function_name}")
        except Exception as e:
            logger.error(f"Failed to delete Lambda {function_name}: {e}")


# HELPER FUNCTIONS ---------------------------------------------------


def add_to_deployed_resources(
    deployed_resources: Dict, resource_type: str, id: str
) -> Dict:
    """
    Helper to add resources to a deployed resources dictionary
    The deployed resources dictionary contains a list of resource ids for each service
    Will add the service if it doesn't already exist, otherwise it will append the id

    Args:
        deployed_resources (Dict): the dictionary tracking the deployed resources
        resource_type (str): the type of resources e.g. lambda
        id (str): the identifier of the resources

    Returns:
        Dict: the updated deployed-resources dictionary
    """
    if resource_type not in deployed_resources:
        deployed_resources[resource_type] = [id]
        return deployed_resources
    else:
        deployed_resources[resource_type].append(id)
        return deployed_resources


# INDIVIDUAL DEPLOYMENT FUNCTIONS ---------------------------------------------------


def deploy_ec2(
    config: Dict, deployed_resources: Dict, base_dir: Path
) -> Tuple[Dict, Dict]:
    """
    Deploys the EC2 upload instance

    Args:
        config (Dict): the application config
        deployed_resources (Dict): dictionary tracking deployed resources
        base_dir (Path): root directory of the project

    Returns:
        Tuple[Dict, Dict]: updated config, updated deployed resources dictionary
    """
    # --- create clients
    ec2_client = boto3.client("ec2")
    ssm_client = boto3.client("ssm")

    # --- get config details
    ec2_config = config.get("resources").get("ec2").get("upload-image")
    ami_id = ec2_helpers.get_ami_id(ssm_client, ec2_config.get("ami-name"))

    # --- read deployment script and inject bucket name as environment variable
    app_deployment_script_path = (
        base_dir / "compute" / "upload-app" / "deployment_script.sh"
    )
    with open(app_deployment_script_path, "r") as f:
        deployment_script = f.read()
    deployment_script = deployment_script.replace(
        "{{S3_BUCKET_NAME}}", config["resources"]["s3"]["image-bucket"]["name"]
    )

    ec2_instance_id = ec2_helpers.ec2_create(
        ec2_client,
        ami_id,
        ec2_config.get("instance-profile"),
        ec2_config.get("instance-type"),
        ec2_config.get("min-count"),
        ec2_config.get("max-count"),
        tags=config.get("tags"),
        user_data_script=deployment_script,
    )
    logger.info(f"EC2 instance created: {ec2_instance_id}")
    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "ec2", ec2_instance_id
    )

    # --- add inbound rule to security group
    ec2_helpers.ec2_add_inbound(ec2_client, ec2_config.get("security-group"))
    return config, deployed_resources


def deploy_dynamodb(config: Dict, deployed_resources: Dict) -> Tuple[Dict, Dict]:
    """
    Deploys the DynamoDB table and enables a new-item stream on it
    Stores the stream ARN in config["runtime"] for use by later deploy steps

    Args:
        config (Dict): the application config
        deployed_resources (Dict): dictionary tracking deployed resources

    Returns:
        Tuple[Dict, Dict]: updated config, updated deployed resources dictionary
    """
    # --- create clients
    dynamodb_client = boto3.client("dynamodb")
    # --- get config details
    dynamodb_config = config.get("resources").get("dynamodb").get("images-db")

    dynamodb_helpers.dynamodb_create_table(
        dynamodb_client,
        dynamodb_config.get("name"),
        dynamodb_config.get("partition-key"),
        dynamodb_config.get("partition-key-type"),
        config.get("tags"),
    )

    logger.info(f"DynamoDB table created: {dynamodb_config.get("name")}")
    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "dynamodb", dynamodb_config.get("name")
    )

    # --- create stream
    dynamodb_helpers.dynamodb_create_new_item_stream(
        dynamodb_client, dynamodb_config.get("name")
    )

    # --- store stream arn in runtime config for lambda trigger setup
    db_stream_arn = dynamodb_helpers.dynamodb_get_stream_arn(
        dynamodb_client, dynamodb_config.get("name")
    )
    config["runtime"]["db_stream_arn"] = db_stream_arn
    logger.info(f"Dynamodb stream created: {db_stream_arn}")

    return config, deployed_resources


def deploy_cloudformation(
    config: Dict, deployed_resources: Dict, base_dir: Path
) -> Tuple[Dict, Dict]:
    """
    Deploys the S3 bucket and SQS queue via CloudFormation
    Stores the SQS queue ARN in config["runtime"] for use by later deploy steps

    Args:
        config (Dict): the application config
        deployed_resources (Dict): dictionary tracking deployed resources
        base_dir (Path): root directory of the project

    Returns:
        Tuple[Dict, Dict]: updated config with runtime queue ARN, updated deployed resources dictionary
    """
    # --- create clients
    cloudformation_client = boto3.client("cloudformation")

    # --- get config details
    cloudformation_config = (
        config.get("resources").get("cloudformation").get("image-bucket-queue")
    )
    stack_name = cloudformation_config.get("name")
    template_path = base_dir / "cloudformation" / "s3-sqs.yaml"
    parameters = {
        "BucketName": config.get("resources").get("s3").get("image-bucket").get("name"),
        "QueueName": config.get("resources").get("sqs").get("notify-image").get("name"),
    }

    cloudformation_helpers.cloudformation_create_stack(
        cloudformation_client,
        stack_name,
        str(template_path),
        parameters,
        tags=config.get("tags"),
    )

    # --- get queue arn for lambda trigger setup
    cloudformation_outputs = cloudformation_helpers.cloudformation_get_outputs(
        cloudformation_client, stack_name
    )
    config["runtime"]["sqs_queue_arn"] = cloudformation_outputs["QueueArn"]
    logger.info(f"SQS queue created: {cloudformation_outputs["QueueArn"]}")

    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "cloudformation", stack_name
    )
    return config, deployed_resources


def deploy_sns(config: Dict, deployed_resources: Dict) -> Tuple[Dict, Dict]:
    """
    Creates the SNS topic and subscribes the configured email address to it
    Stores the topic ARN in config["runtime"] for use by later deploy steps

    Args:
        config (Dict): the application config
        deployed_resources (Dict): dictionary tracking deployed resources

    Returns:
        Tuple[Dict, Dict]: updated config with runtime topic ARN, updated deployed resources dictionary
    """
    # --- create clients
    sns_client = boto3.client("sns")
    # --- get config details
    sns_config = config.get("resources").get("sns").get("email-topic")
    topic_arn = sns_helpers.sns_create_topic(
        sns_client, sns_config.get("name"), config.get("tags")
    )
    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(deployed_resources, "sns", topic_arn)

    # --- store topic arn in runtime config for lambda env var
    config["runtime"]["sns_topic_arn"] = topic_arn
    logger.info(f"SNS topic created: {topic_arn}")

    # --- subscribe the configured email
    email = config.get("email")
    if email:
        sns_helpers.sns_subscribe_email(sns_client, topic_arn, email)
        logger.info(f"SNS topic subscription created for: {email}")
    return config, deployed_resources


def deploy_lambdas(
    config: Dict, deployed_resources: Dict, base_dir: Path
) -> Tuple[Dict, Dict]:
    """
    Deploys both Lambda functions and their triggers
    image-detection is triggered by SQS
    send-email is triggered by DynamoDB stream

    Args:
        config (Dict): the application config
        deployed_resources (Dict): dictionary tracking deployed resources
        base_dir (Path): root directory of the project

    Returns:
        Tuple[Dict, Dict]: updated config, updated deployed resources dictionary
    """
    # --- create clients
    lambda_client = boto3.client("lambda")
    # --- get config details
    lambda_config = config.get("resources").get("lambda")
    dynamodb_config = config.get("resources").get("dynamodb").get("images-db")

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
        tags=config.get("tags"),
        environment_variables=lambda_img_vars,
    )

    # --- create sqs trigger
    lambda_helpers.lambda_create_sqs_trigger(
        lambda_client,
        config.get("runtime").get("sqs_queue_arn"),
        lambda_img_config.get("name"),
    )
    logger.info(f"image-detection Lambda deployed: {lambda_img_config.get("name")}")

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
        tags=config.get("tags"),
        environment_variables=lambda_email_vars,
    )

    # --- create dynamodb stream trigger
    lambda_helpers.lambda_create_dbstream_trigger(
        lambda_client,
        config.get("runtime").get("db_stream_arn"),
        lambda_email_config.get("name"),
    )

    logger.info(f"send-email Lambda deployed: {lambda_email_config.get("name")}")

    # --- add to deployed resources to track
    deployed_resources = add_to_deployed_resources(
        deployed_resources, "lambda", lambda_email_config.get("name")
    )
    return config, deployed_resources
