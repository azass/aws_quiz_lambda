import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import traceback
from decimal import *

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    try:
        quest_id = event["quest_id"]
        # print(quest_id)
        return get_question(event)
    except Exception as e:
        print(quest_id)
        print(f"Error Exception. {type(e)}: {e}")
        print(traceback.format_exc())


def get_question(event):
    quest_id = event["quest_id"]
    table = dynamodb.Table("Question")
    queryData = table.query(KeyConditionExpression=Key("quest_id").eq(quest_id))

    table = dynamodb.Table("QuestionRecord")
    queryData2 = table.query(
        KeyConditionExpression=Key("quest_id").eq(quest_id),
    )

    table = dynamodb.Table("Bug")
    queryData4 = table.query(
        ProjectionExpression="in_question, in_option, in_tag, in_explanation, more_study, memo",
        KeyConditionExpression=Key("quest_id").eq(quest_id),
    )

    question = None
    if queryData["Count"] > 0:
        question = queryData["Items"][0]
        if queryData2["Count"] > 0:
            questionRecord = queryData2["Items"][0]
            if "keywords" in questionRecord:
                questionRecord["keywords"] = get_keywords(questionRecord["keywords"])
            else:
                questionRecord["keywords"] = {}
            questionRecord["tags"] = get_tag_names(
                questionRecord["exam_id"],
                [int(tag_no) for tag_no in questionRecord["tags"]],
            )
            histories = search_answer_histories(quest_id)
            lambdaclient = boto3.client("lambda", "ap-northeast-1")
            resp = lambdaclient.invoke(
                FunctionName="AwsQuizAPI_Retention",
                InvocationType="RequestResponse",
                Payload=json.dumps(
                    {"Method": "calculate", "quest_id": quest_id, "histories": histories},
                    default=lambda _o: int(_o) if isinstance(_o, Decimal) else TypeError,
                ),
            )
            retentionRecord = resp["Payload"].read()
            payload_str = retentionRecord.decode("utf-8")
            retentionRecord = json.loads(payload_str)
            if retentionRecord:
                questionRecord.update(retentionRecord)

            # 辞書同士を結合
            question.update(questionRecord)
            question["histories"] = histories
            if queryData4["Count"] > 0:
                question["is_bug"] = True
                question["bug_points"] = queryData4["Items"][0]

        exam = get_exam(question["exam_id"])
        question.update(exam)

        if "case_id" in question:
            question.update(get_case(question["case_id"]))
        elif "case_id" in event and event["case_id"] != "":
            question.update(get_case(event["case_id"]))

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": question,
    }


def get_keywords(keywords):
    new_keywords = {}
    for term_no, terms in json.loads(keywords).items():
        new_terms = []
        for term in terms:
            queryData = dynamodb.Table("Term").query(
                ProjectionExpression="term_id, word, #level, sort, description, #explain",
                ExpressionAttributeNames={"#level": "level", "#explain": "explain"},
                KeyConditionExpression=Key("term_id").eq(term["term_id"]),
            )
            if queryData["Count"] > 0:
                new_terms.append(queryData["Items"][0])
        new_keywords[term_no] = new_terms
    return new_keywords


def get_tag_names(exam_id, tag_nos):
    # print(tag_nos)
    table = dynamodb.Table("Exam")
    exam = table.query(KeyConditionExpression=Key("exam_id").eq(exam_id))["Items"][0]
    if not tag_nos:
        return []
    table = dynamodb.Table("Tag")
    scan_params = {}
    scan_params["FilterExpression"] = Attr("provider").eq(exam["provider"]) & Attr("tag_no").is_in(
        tag_nos
    )
    scan_params["ProjectionExpression"] = "provider, tag_no, tag_name"
    queryData = table.scan(**scan_params)
    return queryData["Items"]


def get_exam(exam_id):
    table = dynamodb.Table("Exam")
    queryData = table.query(KeyConditionExpression=Key("exam_id").eq(exam_id))
    items = queryData["Items"]
    return items[0]


def search_answer_histories(quest_id):
    # print("START search_answer_histories")
    table = dynamodb.Table("AnswerHistory")
    queryData3 = table.query(
        IndexName="quest_id-index", KeyConditionExpression=Key("quest_id").eq(quest_id)
    )
    if queryData3["Count"] > 0:
        histories = queryData3["Items"]
        # print("histories=", histories)
        histories.sort(key=lambda history: history["answer_date"], reverse=True)
        return histories
    else:
        return []


def get_case(case_id):
    # print(case_id)
    queryData = dynamodb.Table("QuestionCase").query(
        KeyConditionExpression=Key("case_id").eq(case_id)
    )
    # print(queryData)
    if queryData["Count"] > 0:
        return queryData["Items"][0]
    else:
        return {"case_items": []}
