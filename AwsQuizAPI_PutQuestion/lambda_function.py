import json
import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    try:
        logger.info(event)

        table = dynamodb.Table("Question")

        quest_id = event["quest_id"]
        if "question_items" in event:
            question_items = event["question_items"]
            table.update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET question_items = :val",
                ExpressionAttributeValues={":val": question_items},
            )
            logger.info("question_items")
        if "options" in event:
            options = event["options"]
            table.update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET options = :val",
                ExpressionAttributeValues={":val": options},
            )
            logger.info("options")
        if "correct_answer" in event:
            correct_answer = event["correct_answer"]
            table.update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET correct_answer = :val",
                ExpressionAttributeValues={":val": correct_answer},
            )

        if "explanation" in event:
            explanation = event["explanation"]
            table.update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET explanation = :val",
                ExpressionAttributeValues={":val": explanation},
            )
            logger.info("explanation")

        if "original_url" in event:
            original_url = event["original_url"]
            table.update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET original_url = :val",
                ExpressionAttributeValues={":val": original_url},
            )
            logger.info("original_url")

        if "case_id" in event:
            case_id = event["case_id"]
            table.update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET case_id = :val",
                ExpressionAttributeValues={":val": case_id},
            )
            logger.info("case_id")

        if "case_items" in event:
            dynamodb.Table("QuestionCase").update_item(
                Key={"case_id": event["case_id"]},
                UpdateExpression="SET case_items = :val",
                ExpressionAttributeValues={":val": event["case_items"]},
            )
            logger.info("case_items")

        if "tags" in event:
            tags = event["tags"]
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET tags = :val",
                ExpressionAttributeValues={":val": tags},
            )

        if "keywords" in event:
            keywords = event["keywords"]
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET keywords = :val",
                ExpressionAttributeValues={":val": keywords},
            )
            # dynamodb.Table("Question").update_item(
            #     Key={
            #         'quest_id': quest_id
            #     },
            #     UpdateExpression="SET keywords = :val",
            #     ExpressionAttributeValues={
            #         ':val': keywords
            #     }
            # )

        if "more_study" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET more_study = :val",
                ExpressionAttributeValues={":val": event["more_study"]},
            )

        if "is_easy" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET is_easy = :val",
                ExpressionAttributeValues={":val": event["is_easy"]},
            )

        if "is_difficult" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET is_difficult = :val",
                ExpressionAttributeValues={":val": event["is_difficult"]},
            )

        if "is_weak" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET is_weak = :val",
                ExpressionAttributeValues={":val": event["is_weak"]},
            )

        if "is_mandatory" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET is_mandatory = :val",
                ExpressionAttributeValues={":val": event["is_mandatory"]},
            )

        if "is_indefinite" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET is_indefinite = :val",
                ExpressionAttributeValues={":val": event["is_indefinite"]},
            )

        if "learning_note" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET learning_note = :val",
                ExpressionAttributeValues={":val": event["learning_note"]},
            )

        if "labels" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET labels = :val",
                ExpressionAttributeValues={":val": event["labels"]},
            )

        if "scoring" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET scoring = :val",
                ExpressionAttributeValues={":val": event["scoring"]},
            )

        if "maturity" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET maturity = :val",
                ExpressionAttributeValues={":val": event["maturity"]},
            )

        if "priority" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET priority = :val",
                ExpressionAttributeValues={":val": event["priority"]},
            )

        if "is_bug" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET is_bug = :val",
                ExpressionAttributeValues={":val": event["is_bug"]},
            )
            if event["is_bug"] == False:
                dynamodb.Table("QuestionRecord").update_item(
                    Key={"quest_id": quest_id},
                    UpdateExpression="SET more_study = :val",
                    ExpressionAttributeValues={":val": event["is_bug"]},
                )

            dynamodb.Table("Bug").delete_item(Key={"quest_id": quest_id})

        if "is_old" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET is_old = :val",
                ExpressionAttributeValues={":val": event["is_old"]},
            )
            mente_exam_count(quest_id)

        if "not_ready" in event:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET not_ready = :val",
                ExpressionAttributeValues={":val": event["not_ready"]},
            )

        return

    except Exception as e:
        logger.exception(e)
        return {"statusCode": 500, "body": json.dumps("failed")}


def get_exam_id(quest_id):
    queryData = dynamodb.Table("QuestionRecord").query(
        ProjectionExpression="exam_id",
        KeyConditionExpression=Key("quest_id").eq(quest_id),
    )
    if queryData["Count"] == 1:
        return queryData["Items"][0]["exam_id"]
    else:
        None


def mente_exam_count(quest_id):
    exam_id = get_exam_id(quest_id)
    options = {
        "Select": "COUNT",
        "FilterExpression": Attr("exam_id").eq(exam_id) & Attr("is_old").ne(True),
    }
    res = dynamodb.Table("QuestionRecord").scan(**options)

    dynamodb.Table("Exam").update_item(
        Key={"exam_id": exam_id},
        UpdateExpression="SET exam_count = :val",
        ExpressionAttributeValues={":val": res["Count"]},
    )
