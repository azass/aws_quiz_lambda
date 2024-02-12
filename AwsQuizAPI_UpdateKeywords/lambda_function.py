import boto3
from boto3.dynamodb.conditions import Key
import json
import traceback


def lambda_handler(event, context):
    try:
        # print(event)
        provider = event["provider"]
        tag_no = event["tag_no"]
        tag_keywords = event["tag_keywords"]
        quest_id = event["quest_id"]
        quest_keywords = event["quest_keywords"]

        terms = json.loads(tag_keywords) if tag_keywords != "" else None
        quest_terms = json.loads(quest_keywords) if quest_keywords != "" else None

        dynamodb = boto3.resource("dynamodb")

        table = dynamodb.Table("Tag")
        tag_name = table.query(
            ProjectionExpression="tag_name",
            KeyConditionExpression=Key("provider").eq(provider) & Key("tag_no").eq(int(tag_no)),
        )["Items"][0]["tag_name"]

        if terms is not None:
            table = dynamodb.Table("Term")
            update_dict = {}
            for term in terms:
                if "changed" in term:
                    if "description" not in term:
                        term["description"] = []
                    if "explain" not in term:
                        term["explain"] = ""

                    if term["changed"] == "update":
                        last_term = table.query(
                            KeyConditionExpression=Key("term_id").eq(term["term_id"])
                        )["Items"][0]
                        quest_ids = last_term["quest_ids"] if "quest_ids" in last_term else []
                        for _quest_id in quest_ids:
                            if _quest_id in update_dict:
                                update_dict[_quest_id].append(term)
                            else:
                                update_dict[_quest_id] = [term]

                        table.update_item(
                            Key={"term_id": term["term_id"]},
                            UpdateExpression="SET word = :val1, #name2 = :val2, sort = :val3, description = :val4, #name5 = :val5",
                            ExpressionAttributeNames={"#name2": "level", "#name5": "explain"},
                            ExpressionAttributeValues={
                                ":val1": term["word"],
                                ":val2": term["level"],
                                ":val3": term["sort"],
                                ":val4": term["description"],
                                ":val5": term["explain"],
                            },
                        )
                    elif term["changed"] == "new":
                        if "ref" in term:
                            table.put_item(
                                Item={
                                    "term_id": term["term_id"],
                                    "word": term["word"],
                                    "level": term["level"],
                                    "explain": term["explain"],
                                    "sort": term["sort"],
                                    "provider": provider,
                                    "tag_no": int(tag_no),
                                    "description": term["description"],
                                    "ref": term["ref"],
                                }
                            )
                        else:
                            table.put_item(
                                Item={
                                    "term_id": term["term_id"],
                                    "word": term["word"],
                                    "level": term["level"],
                                    "explain": term["explain"],
                                    "sort": term["sort"],
                                    "provider": provider,
                                    "tag_no": int(tag_no),
                                    "description": term["description"],
                                }
                            )
                    elif term["changed"] == "delete":
                        table.delete_item(Key={"term_id": term["term_id"]})

                # 再ソートはクライアントで
                # Question の用語
                # if quest_terms is not None:
                #     for quest_term in [d for d in quest_terms if d.get('term_id') == term['term_id']]:
                #         quest_term['sort'] = term['sort']

            for update_quest_id, update_terms in update_dict.items():
                table = dynamodb.Table("QuestionRecord")
                last_question_record = table.query(
                    ProjectionExpression="keywords",
                    KeyConditionExpression=Key("quest_id").eq(update_quest_id),
                )["Items"][0]
                if "keywords" in last_question_record:
                    last_question_keywords_dic = json.loads(last_question_record["keywords"])
                    # 編集中のタグのみ
                    last_question_keywords = (
                        last_question_keywords_dic[tag_name]
                        if tag_name in last_question_keywords_dic
                        else last_question_keywords_dic[str(tag_no)]
                    )
                    for update_term in update_terms:
                        for quest_term in [
                            d
                            for d in last_question_keywords
                            if d.get("term_id") == update_term["term_id"]
                        ]:
                            quest_term["word"] = update_term["word"]
                            quest_term["level"] = update_term["level"]
                            quest_term["explain"] = update_term["explain"]
                            quest_term["sort"] = update_term["sort"]
                    last_question_keywords.sort(key=lambda term: term["sort"])
                    table.update_item(
                        Key={"quest_id": update_quest_id},
                        UpdateExpression="SET keywords = :val",
                        ExpressionAttributeValues={
                            ":val": json.dumps(last_question_keywords_dic, ensure_ascii=False)
                        },
                    )

        if quest_id != "" and quest_terms is not None:
            simple_terms = {
                k: list(
                    map(
                        lambda t: {
                            tk: tv
                            for tk, tv in t.items()
                            if (tk == "term_id" or tk == "word" or tk == "level")
                        },
                        v,
                    )
                )
                for k, v in quest_terms.items()
            }
            dynamodb.Table("QuestionRecord").update_item(
                Key={"quest_id": quest_id},
                UpdateExpression="SET keywords = :val",
                ExpressionAttributeValues={":val": json.dumps(simple_terms, ensure_ascii=False)},
            )
            # dynamodb.Table("Question").update_item(
            #     Key={
            #         'quest_id': quest_id
            #     },
            #     UpdateExpression="SET keywords = :val",
            #     ExpressionAttributeValues={
            #         ':val': quest_keywords
            #     }
            # )

            quest_tag_terms = (
                quest_terms[tag_name] if tag_name in quest_terms else quest_terms[str(tag_no)]
            )
            table = dynamodb.Table("Term")
            for quest_tag_term in quest_tag_terms:
                term_quest_ids = table.query(
                    KeyConditionExpression=Key("term_id").eq(quest_tag_term["term_id"])
                )["Items"][0]
                if "quest_ids" not in term_quest_ids:
                    term_quest_ids["quest_ids"] = []
                if quest_id not in term_quest_ids["quest_ids"]:
                    term_quest_ids["quest_ids"].append(quest_id)
                    table.update_item(
                        Key={"term_id": quest_tag_term["term_id"]},
                        UpdateExpression="SET quest_ids = :val",
                        ExpressionAttributeValues={":val": term_quest_ids["quest_ids"]},
                    )

    except Exception as e:
        print(f"Error Exception. type={type(e)}: {e}")
        print(traceback.format_exc())
