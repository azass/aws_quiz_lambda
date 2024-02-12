import re
import boto3
from datetime import datetime, timedelta, timezone
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import traceback
import random

num_of_times_to_complete = 4

p = re.compile("[a-zA-Z]+")

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    try:
        if event["Method"] == "SEARCH_QUESTIONS":
            queryData = search_questions(
                event["Args"]["exam_ids"],
                event["Args"]["category_ids"],
                event["Args"]["execute_times"],
                event["Args"]["mistake_times"],
                event["Args"]["correct_times"],
                event["Args"]["no_of_questions"],
                event["Args"]["other_options"],
                event["Args"]["priorities"],
                event["Args"]["exclusives"],
                event["Args"]["scorings"],
                event["Args"]["target_days_agos"],
                event["Args"]["retention"] if "retention" in event["Args"] else -1,
                event["Args"]["order"],
                event["Args"]["except_old"],
                event["Args"]["except_not_ready"] if "except_not_ready" in event["Args"] else False,
            )
            return queryData
        elif event["Method"] == "PRACTICE_STATE":
            return get_practice_state(event["exam_id"])

    except Exception as e:
        print("Error Exception.")
        print(type(e))
        print(e)
        print(traceback.format_exc())


def search_questions(
    exam_ids,
    category_ids,
    execute_times,
    mistake_times,
    correct_times,
    no_of_questions,
    other_options,
    priorities,
    exclusives,
    scorings,
    target_days_agos,
    retention,
    sort_order,
    except_old,
    except_not_ready,
):
    scan_params = {}

    # filter_expression = None
    # if len(exam_ids) > 0:
    #     scan_params['FilterExpression'] = Attr('exam_id').is_in(exam_ids)

    # queryData = dynamodb.Table("QuestionRecord").scan(**scan_params)
    items = []
    for examId in exam_ids:
        queryData = dynamodb.Table("QuestionRecord").query(
            IndexName="exam_id-index", KeyConditionExpression=Key("exam_id").eq(examId)
        )
        items.extend(queryData["Items"])

        while "LastEvaluatedKey" in queryData:
            scan_params["ExclusiveStartKey"] = queryData["LastEvaluatedKey"]
            queryData = dynamodb.Table("QuestionRecord").scan(**scan_params)
            items.extend(queryData["Items"])

    if except_old:
        items = list(filter(lambda x: "is_old" not in x or not x["is_old"], items))

    if except_not_ready:
        items = list(filter(lambda x: "not_ready" not in x or not x["not_ready"], items))

    if len(execute_times) > 0:
        items = list(
            filter(
                lambda x: x["execute_count"] in execute_times
                or (
                    num_of_times_to_complete + 1 in execute_times
                    and x["execute_count"] > num_of_times_to_complete
                ),
                items,
            )
        )

    if len(mistake_times) > 0:
        items = list(
            filter(
                lambda x: x["mistake_count"] in mistake_times
                or (
                    num_of_times_to_complete + 1 in mistake_times
                    and x["mistake_count"] > num_of_times_to_complete
                ),
                items,
            )
        )

    if len(correct_times) > 0:
        items = list(
            filter(
                lambda x: x["correct_count"] in correct_times
                or (
                    num_of_times_to_complete + 1 in correct_times
                    and x["correct_count"] > num_of_times_to_complete
                ),
                items,
            )
        )

    if len(category_ids) > 0:
        str_category_ids = list(map(str, category_ids))
        items = list(filter(lambda x: len(set(x["tags"]) & set(str_category_ids)) > 0, items))

    if len(priorities) > 0:
        items = list(
            filter(
                lambda x: x["priority"] in priorities if "priority" in x else 2 in priorities, items
            )
        )

    if len(scorings) > 0:
        items = list(filter(lambda x: x["scoring"] in scorings, items))

    if retention >= 0:
        if retention == 0:
            items = list(filter(lambda x: "retention" in x and x["retention"] == 0, items))
        else:
            items = list(filter(lambda x: "retention" in x and x["retention"] < retention, items))

    today = datetime.now(timezone(timedelta(hours=+9), "JST"))
    s_today = today.strftime("%Y-%m-%d")

    if len(other_options) > 1 or (len(other_options) == 1 and 5 not in other_options):
        items = list(filter(lambda x: filter_other_options(x, other_options), items))

    if 5 in other_options:
        items = list(filter(lambda x: "halving_date" in x and x["halving_date"] <= s_today, items))

    history_set = set([])

    if len(exclusives) > 0:
        if exclusives[0] < 7:
            for exclusive in exclusives:
                d1 = today - timedelta(days=exclusive)
                histories = search_day_history(d1.strftime("%Y-%m-%d"))
                history_set = history_set.union([history["quest_id"] for history in histories])
        else:
            for exclusive in range(exclusives[0]):
                d1 = today - timedelta(days=exclusive)
                histories = search_day_history(d1.strftime("%Y-%m-%d"))
                history_set = history_set.union([history["quest_id"] for history in histories])

        items = list(filter(lambda x: x["quest_id"] not in history_set, items))

    history_set = set([])
    if len(target_days_agos) > 0:
        filter_items = []
        for target in range(8):
            d1 = today - timedelta(days=target)
            histories = search_day_history(d1.strftime("%Y-%m-%d"))
            history_ex_set = set([history["quest_id"] for history in histories])
            history_mis_set = set(
                [history["quest_id"] for history in histories if history["judgment"] == False]
            )

            if target in target_days_agos:
                filter_items.extend(list(filter(lambda x: x["quest_id"] in history_mis_set, items)))
            items = list(filter(lambda x: x["quest_id"] not in history_ex_set, items))
        items = filter_items

    if sort_order == 0:
        items.sort(key=lambda item: item["quest_id"])
    elif sort_order == 1:
        items.sort(key=lambda item: item["quest_id"], reverse=True)
    else:
        random.seed(10)
        random.shuffle(items)

    resultItems = items
    if no_of_questions > 0:
        resultItems = items[:no_of_questions]

    return resultItems


def filter_other_options(item, other_options):
    result = False
    flg = -2 not in other_options
    for i in range(0, len(other_options)):
        if other_options[i] == 0:
            result = result or ("more_study" in item and item["more_study"])
        elif other_options[i] == 1:
            result = result or ("is_difficult" in item and item["is_difficult"])
        elif other_options[i] == 2:
            result = result or ("is_weak" in item and item["is_weak"])
        elif other_options[i] == 3:
            result = result or ("is_mandatory" in item and item["is_mandatory"])
        elif other_options[i] == 4:
            result = result or ("is_bug" in item and item["is_bug"])
    return result if flg else not result


def get_practice_state(exam_id):
    execute_state = []
    mistake_state = []
    history_done_set = set([])
    today = datetime.now(timezone(timedelta(hours=+9), "JST"))

    for day in range(30):
        d1 = today - timedelta(days=day)
        histories = search_day_history(d1.strftime("%Y-%m-%d"))
        histories = list(filter(lambda x: x["quest_id"].startswith(exam_id), histories))
        history_exe_set = set([history["quest_id"] for history in histories])
        history_mis_set = set(
            [history["quest_id"] for history in histories if history["judgment"] == False]
        )
        execute_items = list(filter(lambda x: x not in history_done_set, history_exe_set))
        mistake_items = list(filter(lambda x: x not in history_done_set, history_mis_set))
        execute_state.append(execute_items)
        mistake_state.append(mistake_items)
        history_done_set |= set([history["quest_id"] for history in histories])

    return {"execute": execute_state, "mistake": mistake_state}


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
