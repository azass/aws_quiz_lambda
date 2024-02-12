import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    try:
        logger.info(event)
        dynamodb.Table("Question").put_item(
            Item={
                "quest_id": event["quest_id"],
                "exam_id": event["exam_id"],
                "exam_no": event["quest_no"],
            }
        )

        dynamodb.Table("QuestionRecord").put_item(
            Item={
                "quest_id": event["quest_id"],
                "exam_id": event["exam_id"],
                "quest_no": event["quest_no"],
                "not_ready": True,
                "execute_count": 0,
                "correct_count": 0,
                "mistake_count": 0,
                "maturity": 1,
                "priority": 2,
                "scoring": 0,
                "tags": [],
            }
        )

        options = {
            "Select": "COUNT",
            "FilterExpression": Attr("exam_id").eq(event["exam_id"]),
        }
        res = dynamodb.Table("QuestionRecord").scan(**options)

        dynamodb.Table("Exam").update_item(
            Key={"exam_id": event["exam_id"]},
            UpdateExpression="SET exam_count = :val",
            ExpressionAttributeValues={":val": res["Count"]},
        )

    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps("failed")}
