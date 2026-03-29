# Operations to manage sns topics
import boto3
from typing import Dict


def sns_create_topic(
    sns_client: boto3.client, topic_name: str, tags: Dict = None
) -> str:
    """
    Creates an sns topic

    Args:
        sns_client (boto3.client): a boto3 sns client instance
        topic_name (str): name of the topic
        tags (Dict, optional): tags to be added to the resource. Defaults to None.

    Returns:
        str: the topic ARN
    """
    # --- create specification dict to add params to pass into creation
    # --- other params are optional
    instance_specification = {"Name": topic_name}

    # OPTIONALS ------------------
    # --- add tags if given
    if tags:
        instance_specification["Tags"] = [
            {"Key": k, "Value": v} for k, v in tags.items()
        ]

    response = sns_client.create_topic(**instance_specification)
    return response["TopicArn"]


def sns_subscribe_email(sns_client: boto3.client, topic_arn: str, email: str) -> str:
    """
    Subscribes an email to a topic

    Args:
        sns_client (boto3.client): a boto3 sns client instance
        topic_arn (str): the topic arn
        email (str): the email

    Returns:
        str: the subscription response
    """
    response = sns_client.subscribe(
        TopicArn=topic_arn, Protocol="email", Endpoint=email
    )
    return response["SubscriptionArn"]
