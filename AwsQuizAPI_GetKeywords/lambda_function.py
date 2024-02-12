import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    try:
        provider = event['provider']
        if 'tag_no' in event:
            tag_no = event['tag_no']
            return get_keywords(provider, int(tag_no))
        else:
            word = event['word']
            return suggest_terms(provider, word)
    except Exception as e:
        print(f"Error Exception.type={type(e)}: {e}")


def get_keywords(provider, tag_no):
    table = dynamodb.Table("Term")
    queryData = table.query(
        IndexName='tag_no-provider-index',
        KeyConditionExpression=Key('provider').eq(provider) & Key('tag_no').eq(tag_no)
    )
    terms = queryData['Items']
    terms.sort(key=lambda term: term['sort'])

    if len(terms) > 0:
        return terms
    else:
        return []


def suggest_terms(provider, word):
    table = dynamodb.Table("Term")
    queryData = table.scan(
        FilterExpression=Attr('provider').eq(provider) & Attr('word').begins_with(word) & Attr('ref').not_exists()
    )
    terms = queryData['Items']
    terms.sort(key=lambda term: term['sort'])

    if len(terms) > 0:
        return terms
    else:
        return []