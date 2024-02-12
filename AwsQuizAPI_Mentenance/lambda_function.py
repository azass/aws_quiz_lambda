import json
import re
import traceback
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import uuid
from datetime import datetime, timedelta, timezone

dynamodb = boto3.resource("dynamodb")
num_of_times_to_complete = 4

tag_dict = {}


def lambda_handler(event, context):
    print(event)
    print(event["Method"])
    try:
        if event["Method"] == "MENTE":
            provider = event["Args"]["provider"]
            exam_id = event["Args"]["exam_id"]
            mente1(provider, exam_id)
            print("complete")

        elif event["Method"] == "setupReportItemBatch":
            provider = event["Args"]["provider"]
            exam_id = event["Args"]["exam_id"]
            setupReportItemBatch(provider, exam_id)
            print("complete")
        elif event["Method"] == "setupReportItem":
            provider = event["Args"]["provider"]
            exam_id = event["Args"]["exam_id"]
            setupReportItem(provider, exam_id)
            print("complete")
        elif event["Method"] == "setupReportItemSns":
            provider = event["Args"]["provider"]
            exam_id = event["Args"]["exam_id"]
            setupReportItem(provider, exam_id, True)
            print("complete")
        elif event["Method"] == "setup_report_item":
            tag_no = event["tag_no"]
            tag_name = event["tag_name"]
            rec = event["rec"]
            setup_report_item(tag_no, tag_name, rec)
            print("complete")

        elif event["Method"] == "setup_report_item_of":
            print("setup_report_item_of")
            provider = event["Args"]["provider"]
            exam_id = event["Args"]["exam_id"]
            tag_no = event["Args"]["tag_no"]
            setup_report_item_of(provider, str(tag_no), exam_id)
            print("com")

        elif event["Method"] == "updateQuestionRecordFromAnswerHistory":
            exam_id = event["Args"]["exam_id"]
            updateQuestionRecordFromAnswerHistory(exam_id)

        elif event["Method"] == "MENTE3":
            exam_id = event["Args"]["exam_id"]
            mente3(exam_id)

        elif event["Method"] == "MENTE4":
            exam_id = event["Args"]["exam_id"]
            mente4(exam_id)

        elif event["Method"] == "MENTE_EXAM_COUNT":
            mente_exam_count(event["Args"]["exam_id"])

        elif event["Method"] == "BACKUP_TAG":
            backupTag()

        elif event["Method"] == "ADD_TEST_DATE":
            i = 0
            quest_id = event["Args"]["quest_id"]
            table = dynamodb.Table("AnswerHistory")
            scanData = table.query(
                IndexName="quest_id-index", KeyConditionExpression=Key("quest_id").eq(quest_id)
            )
            items = scanData["Items"]
            for hist in items:
                if "test_date" not in hist:
                    i += 1
                    hist.setdefault("answered_time", 0)
                    table.put_item(
                        Item={
                            "test_id": hist["test_id"],
                            "quest_id": hist["quest_id"],
                            "test_date": hist["test_id"][:10],
                            "judgment": hist["judgment"],
                            "choice": hist["choice"],
                            "answer_date": hist["answer_date"],
                            "answered_time": hist["answered_time"],
                        }
                    )
                    if i % 100 == 0:
                        print(i)

        elif event["Method"] == "CREATE_TERM":
            createTerm(event["Args"]["provider"], int(event["Args"]["tag_no"]))

        elif event["Method"] == "TRANS_KEYWORDS":
            transKeywords(event["provider"], event["exam_id"])

        elif event["Method"] == "RECOUNT_EXAM_POINT":
            recount_exam_point(event["Args"]["exam_id"])

        elif event["Method"] == "change_tag_no":
            change_tag_no(
                event["Args"]["provider"], event["Args"]["now_tag_no"], event["Args"]["new_tag_no"]
            )

    except Exception as e:
        print(f"Error Exception. {type(e)}: {e}")
        print(traceback.format_exc())


def setup_tags(provider):
    queryData = dynamodb.Table("Tag").query(
        ProjectionExpression="tag_no, tag_name",
        KeyConditionExpression=Key("provider").eq(provider),
    )
    tags = queryData["Items"]
    for tag in tags:
        tag_no = tag["tag_no"]
        tag_dict[str(tag_no)] = tag["tag_name"]


def get_question_records(exam_id):
    queryData = dynamodb.Table("QuestionRecord").query(
        IndexName="exam_id-index", KeyConditionExpression=Key("exam_id").eq(exam_id)
    )
    items = queryData["Items"]
    scan_params = {}
    while "LastEvaluatedKey" in queryData:
        scan_params["ExclusiveStartKey"] = queryData["LastEvaluatedKey"]
        queryData = dynamodb.Table("QuestionRecord").scan(**scan_params)
        items.extend(queryData["Items"])
    return items
    # table = dynamodb.Table("QuestionRecord")
    # scan_params = {}
    # scan_params['FilterExpression'] = Attr('quest_id').begins_with(exam_id)
    # scanData = table.scan(**scan_params)
    # recs = scanData['Items']
    # while 'LastEvaluatedKey' in scanData:
    #     scan_params['ExclusiveStartKey'] = scanData['LastEvaluatedKey']
    #     scanData = dynamodb.Table("QuestionRecord").scan(**scan_params)
    #     recs.extend(scanData['Items'])
    # return recs


def mente1(provider, exam_id):
    setup_tags(provider)
    recs = get_question_records(exam_id)
    for rec in recs:
        print(rec["quest_id"])
        table2 = dynamodb.Table("QuestionRecord")
        table2.update_item(
            Key={"quest_id": rec["quest_id"]},
            UpdateExpression="SET maturity = :val",
            ExpressionAttributeValues={":val": Decimal("1.0")},
        )
        for tag_no in rec["tags"]:
            setup_report_item(tag_no, rec)


def setupReportItemBatch(provider, exam_id):
    setup_tags(provider)
    recs = get_question_records(exam_id)
    recs = list(filter(lambda x: "is_old" not in x or not x["is_old"], recs))
    count_exam_point(exam_id, recs)
    report_items = {}
    for rec in recs:
        for tag_no in rec["tags"]:
            if tag_no in report_items:
                report_item = report_items[tag_no]
                report_item["question_count"] += 1
                report_item["total_count"] += num_of_times_to_complete
                report_item["execute_count"] += rec["execute_count"]
                report_item["correct_count"] += rec["correct_count"]
                report_item["complete_count"] += (
                    rec["correct_count"]
                    if rec["correct_count"] < num_of_times_to_complete
                    else num_of_times_to_complete
                )
            else:
                report_item = {
                    "exam_id": rec["exam_id"],
                    "tag_no": int(tag_no),
                    "tag_name": tag_dict[tag_no],
                    "question_count": 1,
                    "total_count": num_of_times_to_complete,
                    "execute_count": rec["execute_count"],
                    "correct_count": rec["correct_count"],
                    "complete_count": 0,
                }
                report_items[tag_no] = report_item

    clear_report_item(exam_id)
    with dynamodb.Table("ReportItem").batch_writer() as batch:
        for item in report_items.values():
            item["update_timestamp"] = datetime.now(timezone(timedelta(hours=+9), "JST")).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
            batch.put_item(Item=item)


def setupReportItem(provider, exam_id, sns=False):
    clear_report_item(exam_id)
    setup_tags(provider)
    recs = get_question_records(exam_id)
    recs = list(filter(lambda x: "is_old" not in x or not x["is_old"], recs))
    count_exam_point(exam_id, recs)
    for rec in recs:
        # print(rec['quest_id'])
        for tag_no in rec["tags"]:
            if sns:
                setup_report_item_sns(tag_no, tag_dict[tag_no], rec)
            else:
                setup_report_item(tag_no, tag_dict[tag_no], rec)


def setup_report_item_sns(tag_no, tag_name, rec):
    param = {"Method": "setup_report_item", "tag_no": tag_no, "tag_name": tag_name, "rec": rec}
    client = boto3.client("sns")
    response = client.publish(
        TopicArn="arn:aws:sns:ap-northeast-1:527973456287:setup_report_item",
        Message=json.dumps(
            param,
            default=lambda _o: int(_o) if isinstance(_o, Decimal) else TypeError,
        ),
    )


def clear_report_item(exam_id):
    queryData = dynamodb.Table("ReportItem").query(
        KeyConditionExpression=Key("exam_id").eq(exam_id),
        ScanIndexForward=True,
    )
    items = queryData["Items"]
    for item in items:
        response = dynamodb.Table("ReportItem").delete_item(
            Key={"exam_id": exam_id, "tag_no": item["tag_no"]}
        )


def setup_report_item_of(provider, tag_no, exam_id):
    setup_tags(provider)
    recs = get_question_records(exam_id)
    for rec in recs:
        for _tag_no in rec["tags"]:
            if _tag_no == tag_no:
                setup_report_item(tag_no, tag_dict[tag_no], rec)


def setup_report_item(tag_no, tag_name, rec):
    # print(tag_dict[tag_no])
    table = dynamodb.Table("ReportItem")
    queryData = table.query(
        KeyConditionExpression=Key("exam_id").eq(rec["exam_id"]) & Key("tag_no").eq(int(tag_no))
    )
    items = queryData["Items"]

    if len(items) == 0:
        table.put_item(
            Item={
                "exam_id": rec["exam_id"],
                "tag_no": int(tag_no),
                "tag_name": tag_name,
                "question_count": 1,
                "total_count": num_of_times_to_complete,
                "execute_count": rec["execute_count"],
                "correct_count": rec["correct_count"],
                "complete_count": 0,
            }
        )
    else:
        table.update_item(
            Key={"exam_id": rec["exam_id"], "tag_no": int(tag_no)},
            UpdateExpression="set tag_name = :tagname, execute_count = execute_count + :val1, correct_count = correct_count + :val2, question_count = question_count + :val3, total_count = total_count + :val",
            ExpressionAttributeValues={
                ":tagname": tag_name,
                ":val1": rec["execute_count"],
                ":val2": rec["correct_count"],
                ":val3": 1,
                ":val": num_of_times_to_complete,
            },
        )
    complete_count = rec["correct_count"]
    if complete_count >= num_of_times_to_complete:
        complete_count = num_of_times_to_complete

    table.update_item(
        Key={"exam_id": rec["exam_id"], "tag_no": int(tag_no)},
        UpdateExpression="set complete_count = complete_count + :val",
        ExpressionAttributeValues={":val": complete_count},
    )


def updateQuestionRecordFromAnswerHistory(exam_id):
    # table = dynamodb.Table("QuestionRecord")
    # scan_params = {}
    # scan_params["FilterExpression"] = Attr("quest_id").begins_with(exam_id) & Attr(
    #     "execute_count"
    # ).eq(0)
    # scanData = table.scan(**scan_params)
    # recs = scanData["Items"]
    recs = get_question_records(exam_id)
    for rec in recs:
        # if rec["execute_count"] == 0:
        queryData = dynamodb.Table("AnswerHistory").query(
            IndexName="quest_id-index", KeyConditionExpression=Key("quest_id").eq(rec["quest_id"])
        )
        # table = dynamodb.Table("AnswerHistory")
        # filter_expression = Attr("quest_id").eq(rec["quest_id"])
        # scan_params = {}
        # scan_params["FilterExpression"] = filter_expression
        # scan_params["ProjectionExpression"] = "judgment"
        # queryData3 = table.scan(**scan_params)
        if queryData["Count"] > 0:
            histories = queryData["Items"]
            execute_count = len(histories)
            correct_count = len(
                [history["quest_id"] for history in histories if history["judgment"] == True]
            )
            mistake_count = execute_count - correct_count
            sum = 0
            cnt = 0
            answered_average_time = 0
            for history in histories:
                if history["answered_time"] > 0:
                    sum += history["answered_time"]
                    cnt += 1
            if cnt > 0:
                answered_average_time = int(sum / cnt)

            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": rec["quest_id"]},
                UpdateExpression="SET execute_count = :val1, mistake_count = :val2, correct_count = :val3, answered_average_time = :val4",
                ExpressionAttributeValues={
                    ":val1": execute_count,
                    ":val2": mistake_count,
                    ":val3": correct_count,
                    ":val4": answered_average_time,
                },
            )


def mente3(exam_id):
    table = dynamodb.Table("QuestionRecord")
    scan_params = {}
    scan_params["FilterExpression"] = Attr("quest_id").begins_with(exam_id)
    scanData = table.scan(**scan_params)
    recs = scanData["Items"]
    for rec in recs:
        table2 = dynamodb.Table("QuestionRecord")
        table2.update_item(
            Key={"quest_id": rec["quest_id"]},
            UpdateExpression="SET maturity = :val",
            ExpressionAttributeValues={":val": Decimal("1.0")},
        )


def mente4(exam_id):
    print(exam_id)
    table = dynamodb.Table("QuestionRecord")
    scan_params = {}
    scan_params["FilterExpression"] = Attr("quest_id").begins_with(exam_id)
    scanData = table.scan(**scan_params)
    recs = scanData["Items"]
    for rec in recs:
        if "scoring" not in rec:
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": rec["quest_id"]},
                UpdateExpression="SET scoring = :val",
                ExpressionAttributeValues={":val": 0},
            )


def mente_exam_count(exam_id):
    items = get_valid_question_records(exam_id)

    dynamodb.Table("Exam").update_item(
        Key={"exam_id": exam_id},
        UpdateExpression="SET exam_count = :val",
        ExpressionAttributeValues={":val": len(items)},
    )


def backupTag():
    scanData = dynamodb.Table("Tag2").scan()
    allTag = scanData["Items"]
    table = dynamodb.Table("Tag")
    for tag in allTag:
        table.put_item(Item=tag)


def createTerm(provider, tag_no):
    table = dynamodb.Table("Tag")
    queryData = table.query(
        KeyConditionExpression=Key("provider").eq(provider) & Key("tag_no").eq(tag_no)
    )
    tag = queryData["Items"][0]

    if "keywords" in tag:
        terms = []
        table = dynamodb.Table("Term")
        keywords = json.loads(tag["keywords"])
        print(type(keywords))
        index = 0
        for term in keywords:
            index += 1
            term["term_id"] = getTermId()
            table.put_item(
                Item={
                    "term_id": term["term_id"],
                    "word": term["word"],
                    "level": term["level"],
                    "provider": provider,
                    "tag_no": tag_no,
                    "sort": index,
                }
            )
        #     terms.append(term['term_id'])
        # table = dynamodb.Table("Tag")
        # table.update_item(
        #     Key={
        #         'provider': provider,
        #         'tag_no': int(tag_no)
        #     },
        #     UpdateExpression="SET terms = :val",
        #     ExpressionAttributeValues={
        #         ':val': terms
        #     }
        # )


def getTermId():
    return "trm-" + str(uuid.uuid4())[-6:]


def transKeywords(provider, exam_id):
    print(exam_id)
    table = dynamodb.Table("QuestionRecord")
    scan_params = {}
    scan_params["FilterExpression"] = Attr("exam_id").eq(exam_id)
    scan_params["ProjectionExpression"] = "quest_id, keywords"
    scanData = table.scan(**scan_params)
    recs = scanData["Items"]
    # print(recs)
    for rec in recs:
        # print(rec)
        if "keywords" not in rec:
            quest_id = rec["quest_id"]
            print(quest_id)
            question = dynamodb.Table("Question").query(
                ProjectionExpression="keywords", KeyConditionExpression=Key("quest_id").eq(quest_id)
            )["Items"][0]
            if "keywords" in question:
                keywords = json.loads(question["keywords"])
                for tag_name in keywords.keys():
                    scan_params = {}
                    scan_params["FilterExpression"] = Attr("provider").eq(provider) & Attr(
                        "tag_name"
                    ).eq(tag_name)
                    scan_params["ProjectionExpression"] = "tag_no"
                    scanData = dynamodb.Table("Tag").scan(**scan_params)
                    if scanData["Count"] > 0:
                        tag = scanData["Items"][0]
                        # print(tag)
                        tag_no = tag["tag_no"]
                        terms = keywords[tag_name]
                        new_terms = []
                        for term in terms:
                            scan_params = {}
                            scan_params["FilterExpression"] = (
                                Attr("provider").eq(provider)
                                & Attr("tag_no").eq(tag_no)
                                & Attr("word").eq(term["word"])
                                & Attr("level").eq(term["level"])
                            )
                            scan_params[
                                "ProjectionExpression"
                            ] = "term_id, tag_no, word, #level, sort"
                            scan_params["ExpressionAttributeNames"] = {"#level": "level"}
                            scanData = dynamodb.Table("Term").scan(**scan_params)
                            if scanData["Count"] > 0:
                                new_terms.append(scanData["Items"][0])
                        keywords[tag_name] = new_terms
                        # print(keywords)
                        quest_keywords = json.dumps(
                            keywords, default=decimal_default_proc, ensure_ascii=False
                        )
                dynamodb.Table("QuestionRecord").update_item(
                    Key={"quest_id": quest_id},
                    UpdateExpression="SET keywords = :val",
                    ExpressionAttributeValues={":val": quest_keywords},
                )
                dynamodb.Table("Question").update_item(
                    Key={"quest_id": quest_id},
                    UpdateExpression="SET keywords = :val",
                    ExpressionAttributeValues={":val": quest_keywords},
                )


def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        return int(obj)
    raise TypeError


def recount_exam_point(exam_id):
    items = get_valid_question_records(exam_id)
    count_exam_point(exam_id, items)


def count_exam_point(exam_id, items):
    point = 0
    for item in items:
        point_count = item["correct_count"] if item["correct_count"] < 5 else 4
        point += point_count

    exam_count = len(items)
    level = int((point * 10 / num_of_times_to_complete / exam_count))
    if level > 10:
        level = 10

    dynamodb.Table("Exam").update_item(
        Key={"exam_id": exam_id},
        UpdateExpression="SET point = :val, exam_count = :val2, #level = :val3",
        ExpressionAttributeNames={"#level": "level"},
        ExpressionAttributeValues={":val": point, ":val2": exam_count, ":val3": Decimal(level)},
    )


def get_valid_question_records(exam_id):
    queryData = dynamodb.Table("QuestionRecord").query(
        IndexName="exam_id-index", KeyConditionExpression=Key("exam_id").eq(exam_id)
    )
    items = queryData["Items"]

    items = list(filter(lambda x: "is_old" not in x or not x["is_old"], items))
    return items


def change_tag_no(provider, now_tag_no, new_tag_no):
    queryData = dynamodb.Table("Tag").query(
        KeyConditionExpression=Key("provider").eq(provider) & Key("tag_no").eq(now_tag_no)
    )
    if queryData["Count"] > 0:
        item = queryData["Items"][0]
        item["tag_no"] = new_tag_no
        dynamodb.Table("Tag").delete_item(Key={"provider": provider, "tag_no": now_tag_no})
        dynamodb.Table("Tag").put_item(Item=item)

    queryData = dynamodb.Table("Term").query(
        IndexName="tag_no-provider-index",
        KeyConditionExpression=Key("provider").eq(provider) & Key("tag_no").eq(now_tag_no),
    )
    terms = queryData["Items"]
    for term in terms:
        dynamodb.Table("Term").delete_item(Key={"term_id": term["term_id"]})
        term["tag_no"] = new_tag_no
        dynamodb.Table("Term").put_item(Item=term)

    scan_params = {}
    scan_params["FilterExpression"] = Attr("provider").eq(provider)
    scanData = dynamodb.Table("Exam").scan(**scan_params)
    exams = scanData["Items"]
    for exam in exams:
        exam_id = exam["exam_id"]
        queryData = dynamodb.Table("ReportItem").query(
            KeyConditionExpression=Key("exam_id").eq(exam_id) & Key("tag_no").eq(now_tag_no)
        )
        if queryData["Count"] > 0:
            item = queryData["Items"][0]
            item["tag_no"] = new_tag_no
            dynamodb.Table("ReportItem").delete_item(Key={"exam_id": exam_id, "tag_no": now_tag_no})
            dynamodb.Table("ReportItem").put_item(Item=item)

        queryData = dynamodb.Table("QuestionRecord").query(
            IndexName="exam_id-index", KeyConditionExpression=Key("exam_id").eq(exam_id)
        )
        records = queryData["Items"]
        for record in records:
            tags = record["tags"]
            if str(now_tag_no) in tags:
                tags[tags.index(str(now_tag_no))] = str(new_tag_no)
                if "keywords" in record:
                    keywords = json.loads(record["keywords"])
                    if str(now_tag_no) in keywords:
                        keywords[str(new_tag_no)] = keywords.pop(str(now_tag_no))
                        record["keywords"] = json.dumps(
                            keywords, default=decimal_default_proc, ensure_ascii=False
                        )

                dynamodb.Table("QuestionRecord").delete_item(Key={"quest_id": record["quest_id"]})
                dynamodb.Table("QuestionRecord").put_item(Item=record)
