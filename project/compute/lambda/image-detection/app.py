import json
import boto3
import urllib
from datetime import datetime, timezone
from typing import List, Dict, Any, Decimal
import os

# CONFIG
table_name = os.environ["DB_TABLE_NAME"]
selected_emotions = ["ANGRY", "DISGUSTED"]
selected_labels = ["Driving"]

# CLIENTS/RESOURCES
rekognition_client = boto3.client("rekognition")
dynamodb_resource = boto3.resource("dynamodb")
dynamodb_table = dynamodb_resource.Table(table_name)

def lambda_handler(event, context):
    events = event["Records"]
    # --- loop through in cases messages got batched
    for record in events:
        s3_image_ref = get_s3_image_reference(record)
        detected_emotions = determine_emotions(
            rekognition_client, s3_image_ref, selected_emotions
        )
        detected_labels = determine_labels(
            rekognition_client, s3_image_ref, selected_labels
        )

        # --- explicity handle driving as a field in the table
        driving_detected = "Driving" in detected_labels
        # --- dynamodb cannot take floats, it requires doubles, to prevent precision artifacts, cast to string first
        driving_confidence = Decimal(str(detected_labels.get("Driving", 0)))

        # --- to allow for multiple emotions, add emotion dictionary under emotions
        dynamodb_table.put_item(
            Item={
                "name": s3_image_ref["S3Object"]["Name"],
                "datetime_received": datetime.now(timezone.utc).isoformat(),
                "is-driving": driving_detected,
                "driving_confidence": Decimal(str(driving_confidence)),
                "emotions": detected_emotions,
            }
        )

    return {"statusCode": 200, "body": json.dumps("Processing complete")}


def get_s3_image_reference(record: Any) -> Dict:
    """
    Parse lambda event to get s3 bucket and image key

    Args:
        event (Any): the lambda event from an sqs message triggered by s3

    Returns:
        Dict: a bucket, image reference in the format needed to pass to rekognition
    """
    # --- take the first record, as we are only sending one file per message
    body = json.loads(record["body"])
    s3_record = body["Records"][0]["s3"]
    bucket = s3_record["bucket"]["name"]
    key = urllib.parse.unquote_plus(s3_record["object"]["key"], encoding="utf-8")

    # --- return in format needed for Rekognition
    return {"S3Object": {"Bucket": bucket, "Name": key}}


def determine_emotions(
    rekognition_client: boto3.client,
    image: dict,
    selected_emotions: List[str],
) -> Dict[str, float]:
    """
    Determines if any of a list of emotions is detected in faces in a picture, and retrieving the highest confidence value

    Args:
        rekognition_client (boto3.client): a boto3 rekognition client
        image (dict): an S3Object dict containing bucket and name
        selected_emotions (List[str]): list of emotions to determine

    Returns:
        Dict[str, float]: a set of key, value pairs of emotion, confidence for the highest confidence value of all selected emotions that were detected
    """
    # --- just retrieve emotions
    response = rekognition_client.detect_faces(Image=image, Attributes=["EMOTIONS"])
    faces = response.get("FaceDetails", [])
    # --- init dict to be returned
    # --- as there may be multiple faces, want to detect highest confidence value face
    highest_selected_emotions: Dict[str, float] = {}
    for face in faces:
        for emotion in face.get("Emotions", []):
            emotion_type = emotion["Type"]
            # --- dynamodb cannot take floats, it requires doubles, to prevent precision artifacts, cast to string first
            confidence = Decimal(str(emotion["Confidence"]))
            if emotion_type in selected_emotions:
                if (
                    emotion_type not in highest_selected_emotions
                    or confidence > highest_selected_emotions[emotion_type]
                ):
                    # --- if the emotion hasn't been detected yet, or if it is a higher confidence, add/update it to return value
                    highest_selected_emotions[emotion_type] = confidence

    return highest_selected_emotions


def determine_labels(
    rekognition_client: boto3.client,
    image: dict,
    selected_labels: List[str],
) -> Dict[str, float]:
    """
    Determines if any of a list of labels are detected in a picture

    Args:
        rekognition_client (boto3.client): a boto3 rekognition client
        image (dict): an S3Object dict containing bucket and name
        selected_labels (List[str]): list of labels to determine

    Returns:
        Dict[str, float]: a set of key, value pairs of label, confidence
    """

    # --- make sure selected labels are included
    response = rekognition_client.detect_labels(
        Image=image,
        MinConfidence=80,
        Settings={"GeneralLabels": {"LabelInclusionFilters": selected_labels}},
    )

    found_labels = {}

    for label in response["Labels"]:
        name = label["Name"]
        confidence = label["Confidence"]
        # --- only return labels found that are in selected labels
        if name in selected_labels:
            found_labels[name] = confidence

    return found_labels
