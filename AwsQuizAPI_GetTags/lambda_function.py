import boto3
import traceback
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    try:
        if "exam_id" in event:
            return getExamTags(event["provider"], event["exam_id"])
        else:
            return getProviderTags(event["provider"])
    except Exception as e:
        print("Error Exception.")
        print(e)
        print(traceback.format_exc())


def getProviderTags(provider):
    table = dynamodb.Table("Tag")
    scanData = table.scan()
    items = scanData["Items"]
    items = list(filter(lambda tag: tag["provider"] == provider or tag["provider"] == "COM", items))
    items.sort(key=lambda item: item["sort"])
    return items


def getExamTags(provider, exam_id):
    tag_map = get_tag_map(provider)

    queryData = dynamodb.Table("QuestionRecord").query(
        IndexName="exam_id-index", KeyConditionExpression=Key("exam_id").eq(exam_id)
    )
    recs = queryData["Items"]

    tag_counts = {}
    for rec in recs:
        if "tags" in rec:
            for tag in rec["tags"]:
                tag_counts.setdefault(tag, 0)
                tag_counts[tag] = tag_counts[tag] + 1
    tag_counts_list = sorted(tag_counts.items(), key=lambda x: -x[1])

    tags = []
    for tag_tuple in tag_counts_list:
        tag = {}
        tag["tag_no"] = tag_tuple[0]
        tag["tag_name"] = tag_map[tag_tuple[0]]
        tag["provider"] = provider
        tag["count"] = tag_tuple[1]
        tags.append(tag)
    return tags


def get_tag_map(provider):
    # print(provider)
    queryData = dynamodb.Table("Tag").query(
        ProjectionExpression="tag_no, tag_name",
        KeyConditionExpression=Key("provider").eq(provider),
    )
    tags = queryData["Items"]
    tag_map = {}
    for tag in tags:
        tag_map[str(tag["tag_no"])] = tag["tag_name"]
    # print(tag_map)
    return tag_map
