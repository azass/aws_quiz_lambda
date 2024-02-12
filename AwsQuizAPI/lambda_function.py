import json
import re
import boto3
from datetime import datetime, timedelta, timezone
from boto3.dynamodb.conditions import Key, Attr
from decimal import *
import traceback
import random
import statistics

num_of_times_to_complete = 4

p = re.compile("[a-zA-Z]+")

dynamodb = boto3.resource("dynamodb")


def operation_scan():
    table = dynamodb.Table("Question")
    scanData = table.scan()
    return scanData


def operation_exam(exam_id):
    table = dynamodb.Table("Exam")
    queryData = table.query(KeyConditionExpression=Key("exam_id").eq(exam_id))
    items = queryData["Items"]
    return items


def get_exam(exam_id):
    return operation_exam(exam_id)[0]


def get_question_record(quest_id):
    queryData = dynamodb.Table("QuestionRecord").query(
        KeyConditionExpression=Key("quest_id").eq(quest_id)
    )
    if queryData["Count"] == 1:
        return queryData["Items"][0]
    else:
        None


def get_tag_scoring_table(provider, exam_ids):
    tag_map = get_tag_map(provider)
    records = []
    for exam_id in exam_ids:
        queryData = dynamodb.Table("QuestionRecord").query(
            ProjectionExpression="correct_count, execute_count, retention, tags",
            IndexName="exam_id-index",
            KeyConditionExpression=Key("exam_id").eq(exam_id),
        )
        records.extend(queryData["Items"] if queryData["Count"] > 0 else [])
    tag_records = get_tag_records(records)
    tag_scoring_table = []
    for tag_no, records in tag_records.items():
        item = {}
        tag = tag_map[tag_no]
        item["tag_no"] = tag["tag_no"]
        item["tag_name"] = tag["tag_name"]
        item["sort"] = tag["sort"]
        item["question_count"] = len(records)
        retentions = [record["retention"] if "retention" in record else 0 for record in records]
        item["avg_retention"] = Decimal(statistics.mean(retentions)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        total = sum([record["execute_count"] for record in records])
        item["correct_answer_rate"] = 0
        if total > 0:
            item["correct_answer_rate"] = (
                sum([record["correct_count"] for record in records]) / total
            )
        tag_scoring_table.append(item)
    tag_scoring_table.sort(key=lambda item: item["question_count"], reverse=True)
    return tag_scoring_table


def get_tag_map(provider):
    queryData = dynamodb.Table("Tag").query(   
        ProjectionExpression="tag_no, tag_name, sort",
        KeyConditionExpression=Key("provider").eq(provider),
    )
    tags = queryData["Items"]
    tag_map = {}
    for tag in tags:
        tag_map[str(tag["tag_no"])] = tag
    return tag_map


def get_tag_records(records):
    tagRecords = {}
    for record in records:
        for tag in record["tags"]:
            if tag in tagRecords:
                tagRecords[tag].append(record)
            else:
                tagRecords[tag] = [record]
    return tagRecords


def update_question_record_result(judgment, quest_id, maturity, answered_average_time):
    if judgment:
        which_count = "correct_count"
    else:
        which_count = "mistake_count"
    try:
        dynamodb.Table("QuestionRecord").update_item(
            Key={"quest_id": quest_id},
            UpdateExpression="set execute_count = execute_count + :val, "
            + which_count
            + " = "
            + which_count
            + " + :val, maturity = :val2, pass_date = :val3, answered_average_time = :val4",
            ExpressionAttributeValues={
                ":val": 1,
                ":val2": maturity,
                ":val3": pass_date(maturity),
                ":val4": answered_average_time,
            },
        )
    except Exception as e:
        print("QuestionRecord Exception.")
        print(e)


def pass_date(maturity):
    progress = [1, 1, 2, 3, 7, 14]
    today = datetime.now(timezone(timedelta(hours=+9), "JST"))
    return (today + timedelta(progress[maturity])).isoformat()[0:10]


def record(test_id, quest_id, judgment, choice, answered_time, maturity):
    correct_count_up = 0
    point_up = 0
    response = {}

    put_answer_history(test_id, quest_id, judgment, choice, answered_time)

    histories = search_answer_histories(quest_id)
    sum = 0
    cnt = 0
    for history in histories:
        if history["answered_time"] > 0:
            sum += history["answered_time"]
            cnt += 1
    answered_average_time = int(sum / cnt)

    update_question_record_result(judgment, quest_id, maturity, answered_average_time)

    question = get_question_record(quest_id)
    exam_id = question["exam_id"]
    correct_count = question["correct_count"]
    tags = question["tags"]
    exam = get_exam(exam_id)

    update_report_item_result(exam_id, tags, judgment, correct_count)

    if judgment:
        correct_count_up = 1
        # Point Judge
        if correct_count <= num_of_times_to_complete:
            point_up = 1
            # level = 0
            # if exam['point'] > exam['exam_count']:
            #     q, mod = divmod(
            #         exam['point'] - exam['exam_count'] + 1, exam['exam_count'])
            #     level = 3 * q + (mod * 3 // exam['exam_count']) + 1

            exam["point"] = exam["point"] + 1
            level = int((exam["point"] * 10 / num_of_times_to_complete / exam["exam_count"]))
            if level > 10:
                level = 10
            response["levelup"] = level > exam["level"]
            exam["level"] = level
            update_exam_result(exam_id, exam["point"], level)
        else:
            response["levelup"] = False

    else:
        response["levelup"] = False

    response.update(exam)

    # update DailyRecord
    update_daily_record_result(correct_count_up, point_up)

    response["histories"] = histories[:7]
    return response


def update_exam_result(exam_id, point, level):
    dynamodb.Table("Exam").update_item(
        Key={"exam_id": exam_id},
        UpdateExpression="set point = :val1, #name2 = :val2, update_date = :val3",
        ExpressionAttributeNames={"#name2": "level"},
        ExpressionAttributeValues={
            ":val1": point,
            ":val2": level,
            ":val3": datetime.now(timezone(timedelta(hours=+9), "JST")).isoformat(),
        },
    )


def update_exam_scoring(exam_id, newTotalScoring, newAvgScoring):
    dynamodb.Table("Exam").update_item(
        Key={"exam_id": exam_id},
        UpdateExpression="SET total_scoring = :val1, avg_scoring = :val2",
        ExpressionAttributeValues={":val1": newTotalScoring, ":val2": newAvgScoring},
    )


def update_report_item_result(exam_id, tags, judgment, correct_count):
    for tag_no in tags:
        if tag_no == "":
            continue
        update_set = "set execute_count = execute_count + :val"
        if judgment:
            update_set = update_set + ", correct_count = correct_count + :val"
            if correct_count <= num_of_times_to_complete:
                update_set = update_set + ", complete_count = complete_count + :val"
        try:
            dynamodb.Table("ReportItem").update_item(
                Key={"exam_id": exam_id, "tag_no": int(tag_no)},
                UpdateExpression=update_set,
                ExpressionAttributeValues={":val": 1},
            )
        except Exception as e:
            print("exam_id=" + exam_id + ", tag_no=" + tag_no)
            print(e)


def update_daily_record_result(correct_count_up, point_up):
    today = datetime.now(timezone(timedelta(hours=+9), "JST")).isoformat()[0:10]
    queryData = dynamodb.Table("DailyRecord").query(
        KeyConditionExpression=Key("answer_date").eq(today)
    )
    if queryData["Count"] == 0:
        put_daily_record(today)
    dynamodb.Table("DailyRecord").update_item(
        Key={"answer_date": today},
        UpdateExpression="set execute_count = execute_count + :val1, correct_count = correct_count + :val2, point = point + :val3",
        ExpressionAttributeValues={":val1": 1, ":val2": correct_count_up, ":val3": point_up},
    )


def finish_quiz(answer_date, executed_time):
    table = dynamodb.Table("DailyRecord")
    table.update_item(
        Key={"answer_date": answer_date},
        UpdateExpression="set executed_time = executed_time + :val",
        ExpressionAttributeValues={
            ":val": executed_time,
        },
    )


def search_daily_records(from_date, to_date):
    table = dynamodb.Table("DailyRecord")
    scan_params = {}
    scan_params["FilterExpression"] = Attr("answer_date").gte(from_date) & Attr("answer_date").lte(
        to_date
    )
    scanData = table.scan(**scan_params)
    items = scanData["Items"]
    items.sort(key=lambda item: item["answer_date"], reverse=True)
    return items


def put_daily_record(today):
    table = dynamodb.Table("DailyRecord")
    table.put_item(
        Item={
            "answer_date": today,
            "execute_count": 0,
            "correct_count": 0,
            "point": 0,
            "executed_time": 0,
        }
    )


def register_is_bug(args):
    table = dynamodb.Table("Bug")
    table.delete_item(Key={"quest_id": args["quest_id"]})
    if args["is_bug"]:
        response = table.put_item(Item=args)
    if "more_study" in args:
        dynamodb.Table("QuestionRecord").update_item(
            Key={"quest_id": args["quest_id"]},
            UpdateExpression="SET more_study = :val",
            ExpressionAttributeValues={":val": args["more_study"]},
        )

    dynamodb.Table("QuestionRecord").update_item(
        Key={"quest_id": args["quest_id"]},
        UpdateExpression="SET is_bug = :val",
        ExpressionAttributeValues={":val": args["is_bug"]},
    )


def hide_word(quest_id, word):
    table = dynamodb.Table("Keyword")
    table.put_item(Item={"quest_id": quest_id, "word": word, "hide": True})


def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def put_answer_history(test_id, quest_id, judgment, choice, answered_time):
    dynamodb.Table("AnswerHistory").put_item(
        Item={
            "test_id": test_id,
            "quest_id": quest_id,
            "test_date": test_id[:10],
            "answer_date": datetime.now(timezone(timedelta(hours=+9), "JST")).isoformat(),
            "judgment": judgment,
            "choice": choice,
            "answered_time": answered_time,
        }
    )


def search_answer_histories(quest_id):
    table = dynamodb.Table("AnswerHistory")
    queryData3 = table.query(
        IndexName="quest_id-index", KeyConditionExpression=Key("quest_id").eq(quest_id)
    )
    if queryData3["Count"] > 0:
        histories = queryData3["Items"]
        histories.sort(key=lambda history: history["answer_date"], reverse=True)
        return histories
    else:
        return []


def update_answer_history_scoring(test_id, quest_id, newScoring):
    table = dynamodb.Table("AnswerHistory")
    table.update_item(
        Key={"test_id": test_id, "quest_id": quest_id},
        UpdateExpression="SET scoring = :val, update_timestamp = :val2",
        ExpressionAttributeValues={
            ":val": newScoring,
            ":val2": datetime.now(timezone(timedelta(hours=+9), "JST")).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        },
    )


def update_answer_note(test_id, quest_id, answer_note):
    table = dynamodb.Table("AnswerHistory")
    table.update_item(
        Key={"test_id": test_id, "quest_id": quest_id},
        UpdateExpression="SET answer_note = :val, update_timestamp = :val2",
        ExpressionAttributeValues={
            ":val": answer_note,
            ":val2": datetime.now(timezone(timedelta(hours=+9), "JST")).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        },
    )


def search_day_history(answer_date):
    table = dynamodb.Table("AnswerHistory")
    queryData = table.query(
        IndexName="test_date-index",
        # ProjectionExpression='quest_id, answer_date',
        KeyConditionExpression=Key("test_date").eq(answer_date),
    )
    if queryData["Count"] > 0:
        histories = queryData["Items"]
        histories.sort(key=lambda history: history["answer_date"], reverse=True)
        return histories
    else:
        return queryData["Items"]


def update_history_scoring(test_id, exam_id, quest_id, scoring, newScoring):
    exam = get_exam(exam_id)
    newTotalScoring = (
        exam["total_scoring"] - scoring + newScoring if "total_scoring" in exam else newScoring
    )
    newAvgScoring = round(newTotalScoring / exam["exam_count"], 2)

    update_answer_history_scoring(test_id, quest_id, newScoring)
    update_exam_scoring(exam_id, newTotalScoring, newAvgScoring)
    upsert_daily_exam_record(exam_id, test_id, newTotalScoring, newAvgScoring)
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
    return retentionRecord


def upsert_daily_exam_record(exam_id, test_id, newTotalScoring, newAvgScoring):
    queryData = dynamodb.Table("DailyExamRecord").query(
        KeyConditionExpression=Key("exam_id").eq(exam_id) & Key("answer_date").eq(test_id[:10])
    )
    if queryData["Count"] > 0:
        rec = queryData["Items"]
        dynamodb.Table("DailyExamRecord").update_item(
            Key={"exam_id": exam_id, "answer_date": test_id[:10]},
            UpdateExpression="SET total_scoring = :val1, avg_scoring = :val2",
            ExpressionAttributeValues={":val1": newTotalScoring, ":val2": newAvgScoring},
        )
    else:
        dynamodb.Table("DailyExamRecord").put_item(
            Item={
                "exam_id": exam_id,
                "answer_date": test_id[:10],
                "total_scoring": newTotalScoring,
                "avg_scoring": newAvgScoring,
            }
        )


def lambda_handler(event, context):
    try:
        method = event["Method"]
        print(method)
        if event["Method"] == "EXAM":
            exam_id = event["Args"]["exam_id"]
            queryData = operation_exam(exam_id)
            return queryData

        if method == "TAG_SCORING_TABLE":
            provider = event["Args"]["provider"]
            exam_ids = event["Args"]["exam_ids"]
            return get_tag_scoring_table(provider, exam_ids)

        if event["Method"] == "RECORD":
            test_id = event["Args"]["test_id"]
            quest_id = event["Args"]["quest_id"]
            judgment = bool(event["Args"]["judgment"])
            choice = event["Args"]["choice"]
            answered_time = event["Args"]["answered_time"]
            maturity = event["Args"]["maturity"]
            return record(test_id, quest_id, judgment, choice, answered_time, maturity)

        if event["Method"] == "FINISH_QUIZ":
            answer_date = event["Args"]["answer_date"]
            executed_time = event["Args"]["executed_time"]
            finish_quiz(answer_date, executed_time)

        if event["Method"] == "IS_BUG":
            quest_id = event["Args"]["quest_id"]
            is_bug = bool(event["Args"]["is_bug"])
            register_is_bug(event["Args"])

        if event["Method"] == "HIDE_WORD":
            quest_id = event["Args"]["quest_id"]
            word = event["Args"]["word"]
            hide_word(quest_id, word)

        if event["Method"] == "DAILY_RECORDS":
            from_date = event["Args"]["from_date"]
            to_date = event["Args"]["to_date"]
            return search_daily_records(from_date, to_date)

        if event["Method"] == "PUT_DAILY_RECORD":
            today = event["Args"]["today"]
            return put_daily_record(today)

        if event["Method"] == "DAY_HISTORY":
            answer_date = event["Args"]["answer_date"]
            return search_day_history(answer_date)

        if event["Method"] == "UPDATE_HISTORY_SCORING":
            test_id = event["Args"]["test_id"]
            exam_id = event["Args"]["exam_id"]
            quest_id = event["Args"]["quest_id"]
            scoring = event["Args"]["scoring"]
            newScoring = event["Args"]["newScoring"]
            return update_history_scoring(test_id, exam_id, quest_id, scoring, newScoring)

        if event["Method"] == "UPDATE_ANSWER_NOTE":
            test_id = event["Args"]["test_id"]
            quest_id = event["Args"]["quest_id"]
            answer_note = event["Args"]["answer_note"]
            update_answer_note(test_id, quest_id, answer_note)

        # if event["Method"] == "test_history2":
        #     today = datetime.now(timezone(timedelta(hours=+9), "JST"))
        #     d1 = today - timedelta(days=7)
        #     histories = search_day_history2(d1.strftime("%Y-%m-%d"))
        #     print(histories)

    except Exception as e:
        print("Error Exception.")
        print(type(e))
        print(e)
        print(traceback.format_exc())
