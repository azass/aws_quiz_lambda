import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timedelta, timezone
import traceback
from decimal import *
import statistics

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    try:
        exam_id = event["exam_id"]
        print("event=", event)
        if event["tag_no"]:
            tag_no = event["tag_no"]
            provider = event["provider"]
            return get_report_by_tag(provider, exam_id, tag_no)
        else:
            return get_report(exam_id)
    except Exception as e:
        print(f"Error Exception. {type(e)}: {e}")
        print(traceback.format_exc())


def get_report(exam_id):
    print("get_report")
    exam = get_exam(exam_id)
    tag_map = get_tag_map(exam["provider"])
    report = {}
    report.update(exam)

    queryData = dynamodb.Table("QuestionRecord").query(
        ProjectionExpression="quest_id, scoring, retention, tags",
        IndexName="exam_id-index",
        KeyConditionExpression=Key("exam_id").eq(exam_id),
    )

    records = queryData["Items"] if queryData["Count"] > 0 else []
    report["daily_scorings"] = get_daily_scorings(exam, records)
    report.update(get_scoring_counts(records))
    report.update(get_retention_counts(records))
    tagRetentions = get_tag_retentions(records)

    queryData = dynamodb.Table("ReportItem").query(
        KeyConditionExpression=Key("exam_id").eq(exam_id),
        ScanIndexForward=True,
    )
    items = queryData["Items"]
    for item in items:
        item["tag_name"] = tag_map[item["tag_no"]]["tag_name"]
        item["sort"] = tag_map[item["tag_no"]]["sort"]
        item["provider"] = exam["provider"]
        item["tag_avg_retention"] = Decimal(
            statistics.mean(tagRetentions[str(item["tag_no"])])
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    report["items"] = items

    return report


def get_exam(exam_id):
    queryData = dynamodb.Table("Exam").query(KeyConditionExpression=Key("exam_id").eq(exam_id))
    if queryData["Count"] > 0:
        return queryData["Items"][0]
    else:
        print("nothing")
        return {}


def get_tag_map(provider):
    queryData = dynamodb.Table("Tag").query(
        ProjectionExpression="tag_no, tag_name, sort",
        KeyConditionExpression=Key("provider").eq(provider),
    )
    tags = queryData["Items"]
    tag_map = {}
    for tag in tags:
        tag_map[tag["tag_no"]] = tag
    return tag_map


def get_daily_scorings(exam, records):
    qscoring = 0
    qcount = 0
    for record in records:
        qcount = qcount + 1
        if "scoring" in record:
            qscoring += record["scoring"]

    newAvgScoring = round(qscoring / qcount, 2) if qcount > 0 else 0
    dynamodb.Table("Exam").update_item(
        Key={"exam_id": exam["exam_id"]},
        UpdateExpression="SET total_scoring = :val1, avg_scoring = :val2, exam_count = :val3",
        ExpressionAttributeValues={":val1": qscoring, ":val2": newAvgScoring, ":val3": qcount},
    )

    today = datetime.now(timezone(timedelta(hours=+9), "JST"))
    d1 = today - timedelta(days=7)
    d2 = today - timedelta(days=1)
    start_date = d1.strftime("%Y-%m-%d")
    end_date = d2.strftime("%Y-%m-%d")
    option = {
        "ScanIndexForward": False,
        "ProjectionExpression": "answer_date, avg_scoring",
        "KeyConditionExpression": Key("exam_id").eq(exam["exam_id"])
        & Key("answer_date").between(start_date, end_date),
    }
    queryData = dynamodb.Table("DailyExamRecord").query(**option)
    list = queryData["Items"]
    list.insert(0, {"answer_date": today.strftime("%Y-%m-%d"), "avg_scoring": newAvgScoring})

    queryData2 = dynamodb.Table("DailyExamRecord").query(
        KeyConditionExpression=Key("exam_id").eq(exam["exam_id"])
        & Key("answer_date").eq(start_date)
    )
    if queryData2["Count"] == 0:
        option = {
            "ScanIndexForward": False,
            "KeyConditionExpression": Key("exam_id").eq(exam["exam_id"])
            & Key("answer_date").lt(start_date),
            "FilterExpression": Attr("count").gte(1),
            "Limit": 1,
        }
        queryData2 = dynamodb.Table("DailyExamRecord").query(**option)
        start_avg_scoring = 0.0
        if queryData2["Count"] > 0:
            start_avg_scoring = queryData2["Items"][0]["avg_scoring"]
        list.append({"answer_date": start_date, "avg_scoring": start_avg_scoring})
    return list


def get_scoring_counts(records):
    scoringCountDict = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 0, 0: 0}
    sum = 0
    num = 0
    for record in records:
        num += 1
        if "scoring" in record:
            sum += record["scoring"]
            scoringCountDict[record["scoring"]] += 1
        else:
            scoringCountDict[0] += 1
    scoringCounts = []
    for i in reversed(range(0, 11)):
        scoringCounts.append({"scoring": i, "count": scoringCountDict[i]})

    return {
        "avg_scoring": Decimal(sum / num).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if num > 0
        else 0,
        "scoring_counts": scoringCounts,
    }


def get_retention_counts(records):
    retentionCountDict = {"0": 0, "~20": 0, "~40": 0, "~60": 0, "~80": 0, "~100": 0, "100~": 0}
    sum = 0.0
    num = 0
    for record in records:
        num += 1
        if "retention" in record:
            retention = record["retention"]
            sum = Decimal(sum) + Decimal(retention)
            if retention == 0:
                retentionCountDict["0"] += 1
            elif retention < 20:
                retentionCountDict["~20"] += 1
            elif retention < 40:
                retentionCountDict["~40"] += 1
            elif retention < 60:
                retentionCountDict["~60"] += 1
            elif retention < 80:
                retentionCountDict["~80"] += 1
            elif retention < 100:
                retentionCountDict["~100"] += 1
            elif retention >= 100:
                retentionCountDict["100~"] += 1
        else:
            retentionCountDict["0"] += 1

    retentionCounts = []
    for k, v in retentionCountDict.items():
        retentionCounts.append({"label": k, "count": v})
    return {
        "avg_retention": Decimal(sum / num).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if num > 0
        else 0,
        "retention_counts": retentionCounts,
    }


def get_tag_retentions(records):
    retentionTagDict = {}
    for record in records:
        for tag in record["tags"]:
            if tag in retentionTagDict:
                retentionTagDict[tag].append(record["retention"] if "retention" in record else 0)
            else:
                retentionTagDict[tag] = [record["retention"] if "retention" in record else 0]
    return retentionTagDict


def get_report_by_tag(provider, exam_id, tag_no):
    queryData = dynamodb.Table("QuestionRecord").query(
        ProjectionExpression="correct_count, execute_count, scoring, retention, tags, keywords",
        IndexName="exam_id-index",
        KeyConditionExpression=Key("exam_id").eq(exam_id),
    )
    records = queryData["Items"] if queryData["Count"] > 0 else []
    records_by_tag = get_records_by_tag(tag_no, records)
    termIdRecords = get_term_id_records(tag_no, records_by_tag)
    terms = get_terms_by_tag(provider, tag_no)
    items = []
    for term in terms:
        term["question_count"] = 0
        term["avg_retention"] = 0
        term["correct_answer_rate"] = 0
        if term["term_id"] in termIdRecords:
            records = termIdRecords[term["term_id"]]
            term["question_count"] = len(records)
            retentions = [record["retention"] if "retention" in record else 0 for record in records]
            term["avg_retention"] = Decimal(statistics.mean(retentions)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            total = sum([record["execute_count"] for record in records])
            term["correct_answer_rate"] = 0
            if total > 0:
                term["correct_answer_rate"] = (
                    sum([record["correct_count"] for record in records]) / total
                )
            items.append(term)

    report = {}
    report.update(get_scoring_counts(records_by_tag))
    report.update(get_retention_counts(records_by_tag))
    report["items"] = items
    return report


def get_records_by_tag(tag_no, records):
    records_by_tag = []
    for record in records:
        if "tags" in record:
            for tag in record["tags"]:
                if tag == tag_no:
                    records_by_tag.append(record)
    return records_by_tag


def get_terms_by_tag(provider, tag_no):
    table = dynamodb.Table("Term")
    queryData = table.query(
        ProjectionExpression="term_id, word, #level, sort, description, #explain",
        ExpressionAttributeNames={"#level": "level", "#explain": "explain"},
        IndexName="tag_no-provider-index",
        KeyConditionExpression=Key("provider").eq(provider) & Key("tag_no").eq(int(tag_no)),
    )
    terms = queryData["Items"]
    terms.sort(key=lambda term: term["sort"])

    if len(terms) > 0:
        return terms
    else:
        return []


def get_term_id_records(tag_no, records_by_tag):
    termIdRecordsDict = {}
    for record in records_by_tag:
        if "keywords" in record:
            keywords = json.loads(record["keywords"])
            if tag_no in keywords:
                for term in keywords[tag_no]:
                    term_id = term["term_id"]
                    if term_id in termIdRecordsDict:
                        termIdRecordsDict[term_id].append(record)
                    else:
                        termIdRecordsDict[term_id] = [record]
    return termIdRecordsDict
