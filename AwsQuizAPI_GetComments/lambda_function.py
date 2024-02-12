import json
import boto3
import traceback
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    try:
        table = dynamodb.Table("Comments")
        queryData = table.query(
            ProjectionExpression="comment_items, answer_items",
            KeyConditionExpression=Key('quest_id').eq(event["quest_id"])
        )
        if queryData['Count'] > 0:
            return queryData['Items'][0]
        else:
            empt = {}
            empt['comment_items'] = []
            empt['answer_items'] = []
            return empt
    except Exception as e:
        print("Error Exception.")
        print(e)
        print(traceback.format_exc())
