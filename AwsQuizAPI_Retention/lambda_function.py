import json
import traceback
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta, timezone
from decimal import *

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    try:
        now = datetime.now(timezone(timedelta(hours=+9), "JST"))
        if event["Method"] == "add":
            point = get_point(event["scoring"])
            if "last_point" in event and "last_addPoint" in event:
                return add(
                    event["quest_id"], point, now, event["last_point"], event["last_addPoint"]
                )
            else:
                return add(event["quest_id"], point, now)
        elif event["Method"] == "sub":
            sub(event["quest_id"], now)
        elif event["Method"] == "init":
            init(event["quest_id"], now)
        elif event["Method"] == "all_init":
            all_init(event["exam_id"], now)
        elif event["Method"] == "calculate_total":
            return calculate_total(event["quest_id"], now)
        elif event["Method"] == "calculate":
            return calculate(event["quest_id"], event["histories"], now)
        elif event["Method"] == "calculate_all":
            calculate_all(event["exam_ids"], now)

    except Exception as e:
        print("Error Exception.")
        print(type(e))
        print(e)
        print(traceback.format_exc())


def get_point(scoring):
    point = 0
    if scoring == 10:
        point = 50
    elif scoring == 9:
        point = 40
    elif scoring == 8:
        point = 30
    elif scoring == 7:
        point = 20
    elif scoring == 6:
        point = 10
    return point


def get_halving_time(retention):
    halving_time = 0
    if retention <= 40:
        halving_time = 2
    elif retention > 40 and retention <= 60:
        halving_time = 5
    elif retention > 60 and retention <= 80:
        halving_time = 7
    elif retention > 80 and retention <= 110:
        halving_time = 14
    elif retention > 110:
        halving_time = 30
    return halving_time


def add(quest_id, point, now, last_point=0, last_addPoint=0):
    queryData = dynamodb.Table("QuestionRecord").query(
        ProjectionExpression="retention, halving_time, last_correct_date, halving_date, scoring, oversub",
        KeyConditionExpression=Key("quest_id").eq(quest_id),
    )
    retention = 0.0
    addPoint = 0.0
    halving_time = 0
    if queryData["Count"] > 0:
        questionRecord = queryData["Items"][0]
        if "retention" in questionRecord:
            today = now.date()
            halving_date = to_datetime(questionRecord["halving_date"]).date()
            last_scoring = questionRecord["scoring"]
            if (today - halving_date).days > 0 and last_scoring > 5:
                print("case 1")
                addPoint = questionRecord["oversub"] + point
            elif (today - halving_date).days == 0:
                print("case 2")
                addPoint = point
            else:
                if last_point == 0:
                    print("case 3")
                    last_correct_date = to_datetime(questionRecord["last_correct_date"]).date()
                    addPoint = (
                        point * (today - last_correct_date).days / questionRecord["halving_time"]
                    )
                else:
                    print("case 4")
                    addPoint = point * last_addPoint / last_point

                if addPoint > point:
                    addPoint = point
            retention = (
                Decimal(questionRecord["retention"]) + Decimal(addPoint) - Decimal(last_addPoint)
            )
        else:
            addPoint = point
            retention = addPoint

    halving_time = get_halving_time(retention)
    last_correct_date = now.date()
    halving_date = (now + timedelta(days=halving_time)).date()

    dynamodb.Table("QuestionRecord").update_item(
        Key={"quest_id": quest_id},
        UpdateExpression="set retention = :val1, halving_time = :val2, last_correct_date = :val3, halving_date = :val4, oversub = :val5",
        ExpressionAttributeValues={
            ":val1": Decimal(retention),
            ":val2": halving_time,
            ":val3": last_correct_date.strftime("%Y-%m-%d"),
            ":val4": halving_date.strftime("%Y-%m-%d"),
            ":val5": Decimal(0.0),
        },
    )
    return {
        "retention": Decimal(retention).quantize(Decimal("0"), rounding=ROUND_DOWN),
        "halving_time": halving_time,
        "halving_date": halving_date.strftime("%Y-%m-%d"),
        "last_point": point,
        "last_addPoint": addPoint,
    }


def sub(quest_id, now):
    queryData = dynamodb.Table("QuestionRecord").query(
        ProjectionExpression="retention, halving_time, last_correct_date, halving_date, scoring, oversub",
        KeyConditionExpression=Key("quest_id").eq(quest_id),
    )
    if queryData["Count"] > 0:
        questionRecord = queryData["Items"][0]
        if len(questionRecord) == 0:
            return
        if "retention" in questionRecord and questionRecord["retention"] > 0.0:
            today = now.date()
            halving_date = to_datetime(questionRecord["halving_date"]).date()
            subPoint = Decimal(20.0) / questionRecord["halving_time"]
            retention = Decimal(questionRecord["retention"]) - Decimal(subPoint)
            if retention < 0.0:
                retention = 0.0

            oversub = 0.0
            last_scoring = questionRecord["scoring"]
            if (today - halving_date).days >= 0 and last_scoring > 5:
                if questionRecord["retention"] > subPoint:
                    oversub = Decimal(questionRecord["oversub"]) + Decimal(subPoint)
                else:
                    oversub = Decimal(questionRecord["oversub"]) + Decimal(
                        questionRecord["retention"]
                    )

            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="set retention = :val1, oversub = :val2",
                ExpressionAttributeValues={
                    ":val1": Decimal(retention),
                    ":val2": Decimal(oversub),
                },
            )


def to_datetime(str):
    return datetime.strptime(str, "%Y-%m-%d")


def all_init(exam_id, now):
    queryData = dynamodb.Table("QuestionRecord").query(
        ProjectionExpression="quest_id, retention",
        IndexName="exam_id-index",
        KeyConditionExpression=Key("exam_id").eq(exam_id),
    )
    if queryData["Count"] > 0:
        items = queryData["Items"]
        count = 0
        for item in items:
            # if "retention" in item:
            #     continue
            print(count + 1, ": ", item["quest_id"])
            now1 = datetime.now()
            # init(item["quest_id"], now)
            mente(item["quest_id"])
            now2 = datetime.now()
            print((now2 - now1).seconds, "秒")
            # count += 1
            # if count == 30:
            #     break


def init(quest_id, now):
    dynamodb.Table("QuestionRecord").update_item(
        Key={"quest_id": quest_id},
        UpdateExpression="remove retention, halving_time, last_correct_date, oversub",
    )

    table = dynamodb.Table("AnswerHistory")
    queryData = table.query(
        IndexName="quest_id-index", KeyConditionExpression=Key("quest_id").eq(quest_id)
    )
    if queryData["Count"] > 0:
        histories = queryData["Items"]
        histories.sort(key=lambda history: history["answer_date"])

        for i in range(len(histories)):
            test_date = to_datetime(histories[i]["test_date"])
            if "scoring" in histories[i]:
                point = get_point(histories[i]["scoring"])
                add(quest_id, point, test_date)
                dynamodb.Table("QuestionRecord").update_item(
                    Key={"quest_id": quest_id},
                    UpdateExpression="set scoring = :val1",
                    ExpressionAttributeValues={
                        ":val1": histories[i]["scoring"],
                    },
                )
            while (
                i < len(histories) - 1
                and (test_date - to_datetime(histories[i + 1]["test_date"])).days <= 0
            ) or (i == len(histories) - 1 and (test_date.date() - now.date()).days <= 0):
                sub(quest_id, test_date)
                test_date = test_date + timedelta(days=1)


def calculate_all(exam_ids, now):
    for exam_id in exam_ids:
        calculate_exam(exam_id, now)


def calculate_exam(exam_id, now):
    queryData = dynamodb.Table("QuestionRecord").query(
        ProjectionExpression="quest_id",
        IndexName="exam_id-index",
        KeyConditionExpression=Key("exam_id").eq(exam_id),
    )
    if queryData["Count"] > 0:
        items = queryData["Items"]
        count = 0
        for item in items:
            count += 1
            print(count, ": ", item["quest_id"])
            now1 = datetime.now()
            calculate_total(item["quest_id"], now)
            now2 = datetime.now()
            print((now2 - now1).seconds, "秒")


def calculate_total(quest_id, now):
    table = dynamodb.Table("AnswerHistory")
    queryData = table.query(
        IndexName="quest_id-index", KeyConditionExpression=Key("quest_id").eq(quest_id)
    )
    if queryData["Count"] > 0:
        histories = queryData["Items"]
        return calculate(quest_id, histories, now)


def calculate(quest_id, histories, now):
    histories.sort(key=lambda history: history["answer_date"])

    retention = 0
    last_correct_date = None
    halving_time = 0
    halving_date = None
    current_answer_result = False
    for history in histories:
        test_date = to_datetime(history["test_date"])
        pre_answer_result = current_answer_result
        current_answer_result = "scoring" in history and history["scoring"] > 5
        if current_answer_result:
            if last_correct_date:
                if pre_answer_result and (test_date - halving_date).days >= 0:
                    retention = retention - 20
                else:
                    retention = retention - 20 * (test_date - last_correct_date).days / halving_time

                if retention <= 0:
                    retention = 0

            last_correct_date = test_date
            point = get_point(history["scoring"])
            retention = retention + point
            halving_time = get_halving_time(retention)
            halving_date = last_correct_date + timedelta(days=halving_time)

    today = now.date()
    oversub = 0
    # 連続正解＝＞その間、減衰はない＝＞その分の減衰を戻す必要あり
    # 最後に正解している＝＞連続正解する可能性あり=>減衰の戻しを準備
    if current_answer_result and (today - halving_date.date()).days >= 0:
        oversub = 20 * (today - halving_date.date()).days / halving_time
        if oversub >= retention - 20:
            oversub = retention - 20

    # 前回正解から今日までの減衰を計算
    if last_correct_date:
        retention = retention - 20 * (today - last_correct_date.date()).days / halving_time
        if retention < 0:
            retention = 0

        dynamodb.Table("QuestionRecord").update_item(
            Key={"quest_id": quest_id},
            UpdateExpression="set retention = :val1, halving_time = :val2, last_correct_date = :val3, halving_date = :val4, oversub = :val5, update_timestamp = :val6",
            ExpressionAttributeValues={
                ":val1": Decimal(str(retention)),
                ":val2": halving_time,
                ":val3": last_correct_date.strftime("%Y-%m-%d"),
                ":val4": halving_date.strftime("%Y-%m-%d"),
                ":val5": Decimal(str(oversub)),
                ":val6": datetime.now(timezone(timedelta(hours=+9), "JST")).strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                ),
            },
        )
    return {
        "retention": Decimal(retention).quantize(Decimal("0"), rounding=ROUND_DOWN),
        "halving_time": halving_time,
        "halving_date": halving_date.strftime("%Y-%m-%d") if halving_date else "",
    }


def mente(quest_id):
    table = dynamodb.Table("AnswerHistory")
    queryData = table.query(
        IndexName="quest_id-index", KeyConditionExpression=Key("quest_id").eq(quest_id)
    )
    if queryData["Count"] > 0:
        histories = queryData["Items"]
        histories.sort(key=lambda history: history["answer_date"], reverse=True)
        dynamodb.Table("QuestionRecord").update_item(
            Key={"quest_id": quest_id},
            UpdateExpression="set scoring = :val1",
            ExpressionAttributeValues={
                ":val1": histories[0]["scoring"],
            },
        )
