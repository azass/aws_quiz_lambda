import boto3
from boto3.dynamodb.conditions import Key
import json
import traceback


def lambda_handler(event, context):
    print(event)
    try:
        sort = 0
        if 'sort' in event:
            sort = event['sort']
        dynamodb = boto3.resource('dynamodb')
        dynamodb.Table("Term").update_item(
            Key={
                'term_id': event["term_id"]
            },
            UpdateExpression="SET word = :val1, #name2 = :val2, provider = :val3, tag_no = :val4, description = :val5, sort = :val6",
            ExpressionAttributeNames={
                '#name2': 'level'
            },
            ExpressionAttributeValues={
                ':val1': event['word'],
                ':val2': event['level'],
                ':val3': event['provider'],
                ':val4': event['tag_no'],
                ':val5': event['description'],
                ':val6': sort
            }
        )
    except Exception as e:
        print(f"Error Exception. type={type(e)}: {e}")
        print(traceback.format_exc())
