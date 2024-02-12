import boto3
from boto3.dynamodb.conditions import Key
import json
import traceback

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    try:
        provider = event["provider"]
        quest_id = event["quest_id"]

        return suggestTags(provider, quest_id)

    except Exception as e:
        print(f"Error Exception. type={type(e)}: {e}")
        print(traceback.format_exc())


def suggestTags(provider, quest_id):
    tag_nos = []
    queryData = dynamodb.Table("Question").query(
        ProjectionExpression="question_items, options",
        KeyConditionExpression=Key("quest_id").eq(quest_id),
    )
    question_items = queryData["Items"][0]["question_items"]
    options = queryData["Items"][0]["options"]

    queryData = dynamodb.Table("Tag").query(
        ProjectionExpression="tag_no, tag_name, thesaurus",
        KeyConditionExpression=Key("provider").eq(provider),
    )
    tags = queryData["Items"]

    for option in options:
        if "correct" in option and option["correct"]:
            for tag in tags:
                if tag["tag_name"] in option["text"]:
                    tag_nos.append(tag["tag_no"])
                    continue
                if "thesaurus" in tag:
                    for word in tag["thesaurus"]:
                        if word in option["text"]:
                            tag_nos.append(tag["tag_no"])
                            break

    if provider != "AWS":
        for question_item in question_items:
            if "text" in question_item:
                for tag in tags:
                    if tag["tag_name"] in question_item["text"]:
                        tag_nos.append(tag["tag_no"])
                        continue
                    if "thesaurus" in tag:
                        for word in tag["thesaurus"]:
                            if word in question_item["text"]:
                                tag_nos.append(tag["tag_no"])
                                break

    return tag_nos
