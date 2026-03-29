# Operations to manage dynamodb infrastructure
import boto3
from typing import Dict, List


def dynamodb_create_table(
    dynamodb_client: boto3.client,
    table_name: str,
    partition_key_name: str,
    partition_key_type: str,
    tags: dict = None,
) -> Dict:
    """
    Create a pay per request dynamodb table with a single partition key

    Args:
        dynamodb_client (boto3.client): a boto3 dynamodb client insance
        table_name (str): name of the table
        partition_key_name (str): name of the partition key
        partition_key_type (str): type of the key, one of "S", "N", "B"
        tags (dict, optional): tags to be added to the table. Defaults to None.

    Raises:
        ValueError: when the given key type is not one of "S", "N", "B"

    Returns:
        Dict: creation response
    """

    # --- ensure partition key types valid
    valid_types = {"S", "N", "B"}
    if partition_key_type not in valid_types:
        raise ValueError("Invalid partition key type")
    

    # --- create specification dict to add params to pass into creation
    # --- other params are optional
    instance_specification = {
        "TableName": table_name,
        "AttributeDefinitions": [
            {"AttributeName": partition_key_name, "AttributeType": partition_key_type}
        ],
        "KeySchema": [{"AttributeName": partition_key_name, "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST"
    }

    # --- add tags if given
    if tags:
        instance_specification["Tags"] = [
            {"Key": k, "Value": v} for k, v in tags.items()
        ]

    # --- create table
    response = dynamodb_client.create_table(**instance_specification)

    return response


def dynamodb_create_new_item_stream(
    dynamodb_client: boto3.client, table_name: str
) -> Dict:
    """
    Update a dynamodb table to add a stream for new items in the table

    Args:
        dynamodb_client (boto3.client): a boto3 dynamodb client insance
        table_name (str): name of the table

    Returns:
        Dict: creation response
    """
    # --- create table
    # --- new image streamviewtype for the entirety of a new item
    response = dynamodb_client.update_table(
        TableName=table_name,
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_IMAGE"},
    )
    return response


def dynamodb_delete(dynamodb_client: boto3.client, table_name: str) -> Dict:
    """
    Delete a dynamodb table

    Args:
        dynamodb_client (boto3.client): a boto3 dynamodb client insance
        table_name (str): name of the table

    Returns:
        Dict: deletion response
    """

    response = dynamodb_client.delete_table(TableName=table_name)

    waiter = dynamodb_client.get_waiter("table_not_exists")
    waiter.wait(TableName=table_name)

    return response


def dynamodb_describe(dynamodb_client: boto3.client, table_name: str) -> Dict:
    """
    Get details of a dynamodb table

    Args:
        dynamodb_client (boto3.client): a boto3 dynamodb client insance
        table_name (str): name of the table

    Returns:
        Dict: description of the table
    """
    response = dynamodb_client.describe_table(TableName=table_name)
    return response


def dynamodb_get_stream_arn(dynamodb_client: boto3.client, table_name: str) -> str:
    """
    Gets the stream arn of a dynamodb table

    Args:
        dynamodb_client (boto3.client): a boto3 dynamodb client insance
        table_name (str): name of the table

    Returns:
        str: the stream arn of the dynamodb table
    """

    response = dynamodb_describe(dynamodb_client, table_name)
    return response["Table"].get("LatestStreamArn")
