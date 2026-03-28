import boto3


def sns_create_topic(
    sns_client: boto3.client,
    topic_name: str,
) -> str:
    """
    Creates an sns topic

    Args:
        sns_client (boto3.client): an sns client instance
        topic_name (str): _description_

    Returns:
        str: _description_
    """
    response = sns_client(name=topic_name)
    return response["TopicArn"]


def sns_subscribe_email(sns_client: boto3.client, topic_arn: str, email: str) -> str:
    """
    _summary_

    Args:
        sns_client (boto3.client): _description_
        topic_arn (str): _description_
        email (str): _description_

    Returns:
        str: _description_
    """
    response = sns_client.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    return response["SubscriptionArn"]