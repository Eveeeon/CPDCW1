import json
import boto3
import os

sns_client = boto3.client("sns")

TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

def lambda_handler(event, context):

    for record in event["Records"]:

        if record["eventName"] != "INSERT":
            continue

        new_image = record["dynamodb"]["NewImage"]

        driving = new_image["is-driving"]["BOOL"]
        driving_conf = float(new_image["driving_confidence"]["N"])

        emotions = new_image.get("emotions", {}).get("M", {})

        angry_conf = None

        if "ANGRY" in emotions:
            angry_conf = float(emotions["ANGRY"]["N"])

        if driving and angry_conf and angry_conf > 80:

            message = f"""
Dangerous driving detected

Driving confidence: {driving_conf}
Angry confidence: {angry_conf}
"""

            sns_client.publish(
                TopicArn=TOPIC_ARN,
                Subject="Dangerous Driving Alert",
                Message=message
            )

    return {"statusCode": 200}