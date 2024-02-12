import boto3
from boto3.dynamodb.conditions import Key
import json
import traceback
from datetime import datetime, timedelta, timezone


def lambda_handler(event, context):
    print(event)
    try:
        dynamodb = boto3.resource('dynamodb')
        dynamodb.Table("Comments").update_item(
            Key={
                'quest_id': event["quest_id"]
            },
            UpdateExpression="SET comment_items = :val, answer_items = :val2, title = :val3, src = :val4, update_date = :val5",
            ExpressionAttributeValues={
                ':val': event["comment_items"],
                ':val2': event["answer_items"],
                ':val3': event["title"],
                ':val4': event["source"],
                ':val5': datetime.now(timezone(timedelta(hours=+9), 'JST')).isoformat()[:10]
            }
        )
    except Exception as e:
        print(f"Error Exception. type={type(e)}: {e}")
        print(traceback.format_exc())
