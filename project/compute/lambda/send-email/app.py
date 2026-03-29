import boto3
import os

sns_client = boto3.client("sns")

topic_arn = os.environ["SNS_TOPIC_ARN"]

def lambda_handler(event, context):
    events = event["Records"]
    # --- loop through in cases messages got batched
    for record in events:
        # --- get the new item added to the db
        new_item = record["dynamodb"]["NewImage"]
        to_email = check_email_conditions(new_item)
        if to_email:
            publish_message(sns_client, topic_arn)

def check_email_conditions(new_item: dict) -> bool:
    """
    Checks if the conditions to send the email message have been met

    Args:
        new_item (dict): a single database entry of rekognition results

    Returns:
        bool: if the conditions for emailing have been met
    """
    # --- get driving values
    is_driving = new_item["is-driving"]["BOOL"]
    if not is_driving:
        return False
    driving_confidence = float(new_item["driving_confidence"]["N"])
    if driving_confidence < 80:
        return False

    # --- get emotion values
    emotions = new_item.get("emotions", {}).get("M", {})

    if "ANGRY" not in emotions:
        return False
    angry_confidence = float(emotions.get("ANGRY"))
    if angry_confidence < 80:
        return False
    
    # --- function runs negative checks, if it gets to end, it meets the conditions
    return True

def publish_message(sns_client: boto3.client, topic_arn: str):
    """
    Publishes a driving alert message

    Args:
        sns_client (boto3.client): a boto3 sns client
        topic_arn (str): the arn of the topic
    """
    subject = "Driving Alert"
    message = "Dangerous driving has been detected"
    sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=message
    )
