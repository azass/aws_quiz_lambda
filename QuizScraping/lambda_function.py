import json
import urllib.parse
import boto3
import datetime
from datetime import timedelta, timezone
import random
import os
import requests
from bs4 import BeautifulSoup
from googletrans import Translator
from time import sleep
import re
import traceback

print("Loading function")

s3 = boto3.resource("s3")


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    JST = timezone(timedelta(hours=+9), "JST")
    dt_now = datetime.datetime.now(JST)
    date_str = dt_now.strftime("%Y年%m月%d日")

    data = event["data"]
    # print(data)

    # response = requests.get('https://www.examtopics.com/discussions/google/view/28090-exam-associate-cloud-engineer-topic-1-question-134/')
    # print(response.text)

    # f = open("response.txt", "r")
    # data = f.read()
    # f.close()
    try:

        soup = BeautifulSoup(data, "html.parser")

        translator = Translator(service_urls=["translate.googleapis.com"])

        question = {}
        question["quest_id"] = event["quest_id"]
        titles = soup.find_all('h1')
        if len(titles) > 1:
            question["title"] = titles[1].getText()
        else:
            question["title"] = titles[0].getText()

        question["source"] = data
        quest = soup.find(class_="question-body")

        q = "".join(map(str, quest.find("p").contents)).lstrip()
        # print(q)
        # q_en = quest.find("p").get_text(separator="\n", strip=True)

        # q_jp = translator.translate(q_en, dest="ja").text
        # q = {}
        # q["text"] = q_jp
        # q["text_en"] = q_en
        question_items = []
        for child in re.split('\<br\/\>|\<br\>', q):
            if (child.startswith('<img')):
                s = BeautifulSoup(child)
                src = s.find("img").attrs['src']
                if src.startswith("/"):
                    src = "https://www.examtopics.com" + src
                img = {}
                img['type'] = "image"
                img['image_path'] = src
                img['lv'] = "2"
                question_items.append(img)
            elif len(child) != 0:
                q_en = child
                q_jp = q_en if q_en.startswith(
                    "http") else translator.translate(q_en.replace('.)', ')'), dest="ja").text
                if q_jp:
                    items_len = len(question_items)
                    if items_len == 0 or question_items[items_len - 1]['type'] != "textarea":
                        txt = {}
                        txt['type'] = "textarea"
                        txt['text'] = q_jp.replace('。', '。\n')
                        txt['text_en'] = q_en.rstrip()
                        question_items.append(txt)
                    else:
                        txt = question_items[items_len - 1]
                        txt['text'] = txt['text'] + \
                            "\n" + q_jp.replace('。', '。\n')
                        txt['text_en'] = txt['text_en'] + "\n" + q_en

        for item in reversed(question_items):
            if item['type'] == 'textarea':
                index = item['text'].rfind('?')
                if index == -1:
                    index = item['text'].rfind('？')
                if index != -1:
                    # item['text'] = item['text'][:index + 1]
                    item['text'] = item['text'].rstrip()
                break

        options = []

        for tag in quest.find_all("li", class_="multi-choice-item"):
            # sleep(1)
            option = {}
            a_en = tag.get_text(separator="\n", strip=True)
            option["text_en"] = a_en.rstrip()
            option["text"] = re.sub('^([A-Z])。\n', '\\1. ',
                                    translator.translate(a_en, dest="ja").text).replace('。', '。\n')
            option["text"] = option["text"].rstrip()
            option["mark"] = tag.span.attrs["data-choice-letter"]
            option_img = tag.find("img")
            if option_img:
                src = option_img.attrs['src']
                if src.startswith("/"):
                    src = "https://www.examtopics.com" + src
                option['image_path'] = src
            options.append(option)

        answers = []
        correct_answer = soup.find(class_="correct-answer")
        answer_image = correct_answer.find("img")
        if answer_image:
            src = answer_image.attrs['src']
            if src.startswith("/"):
                src = "https://www.examtopics.com" + src
            img = {}
            img['type'] = "image"
            img['image_path'] = src
            answers.append(img)

        answer = correct_answer.get_text().strip()
        if len(answer) > 0:
            txt = {}
            txt['type'] = "textarea"
            txt['text'] = answer
            answers.append(txt)

        answer_description = soup.find(class_="answer-description")
        if answer_description:
            descs = []
            desc = "".join(map(str, answer_description.contents)).strip()
            for child in re.split('\<br\/\>|\<br\>', desc):
                if (child.startswith('<img')):
                    s = BeautifulSoup(child)
                    src = s.find("img").attrs['src']
                    if src.startswith("/"):
                        src = "https://www.examtopics.com" + src
                    img = {}
                    img['type'] = "image"
                    img['image_path'] = src
                    img['lv'] = "2"
                    descs.append(img)
                elif len(child) != 0:
                    desc_en = child
                    desc_jp = desc_en if desc_en.startswith(
                        "http") else translator.translate(desc_en, dest="ja").text
                    if desc_jp:
                        items_len = len(descs)
                        desc_jp = desc_jp.replace('en-us', 'ja-jp')
                        if items_len == 0 or descs[items_len - 1]['type'] != "textarea":
                            txt = {}
                            txt['type'] = "textarea"
                            txt['text'] = desc_jp.replace('。', '。\n')
                            txt['text_en'] = desc_en
                            descs.append(txt)
                        else:
                            txt = descs[items_len - 1]
                            txt['text'] = txt['text'] + "\n" + \
                                desc_jp.replace('。', '。\n')
                            txt['text_en'] = txt['text_en'] + "\n" + desc_en
            answers.extend(descs)
            # desc = answer_description.get_text().strip()
            # if len(desc):
            #     desc_jp = translator.translate(desc, dest="ja").text.replace('en-us', 'ja-jp')
            #     desc_en = "".join(map(str, answer_description.contents)).strip()
            #     txt = {}
            #     txt['type'] = "textarea"
            #     txt['text'] = desc_jp
            #     txt['text_en'] = desc_en
            #     answers.append(txt)

        discussion = soup.find(class_="discussion-container")
        comments = []
        if discussion:
            comment_list = discussion.find_all(
                class_="comment-container", recursive=False)
            for c in comment_list:
                # print(c.get("id"))
                comments.append(parse(c, translator))

            # for c in comments:
            #     print(c)
        question["question_items"] = question_items
        question["options"] = options
        question["comments"] = comments
        question["answers"] = answers
        # print(question)
        params = {}
        params["quest_id"] = question["quest_id"]
        params["comment_items"] = question["comments"]
        params["answer_items"] = question["answers"]
        params["title"] = question["title"]
        params["source"] = question["source"]
        lambdaclient = boto3.client('lambda', 'ap-northeast-1')
        resp = lambdaclient.invoke(
            FunctionName='AwsQuizAPI_PutComments',
            InvocationType='Event',
            Payload=json.dumps(params)
        )

        return question
    except Exception as e:
        print("Error Exception.")
        print(e)
        print(traceback.format_exc())
        print(data)


def parse(comment_tag, translator):
    comment = {}
    date = comment_tag.find(class_="comment-date")
    badge = comment_tag.find(class_="badge")
    selected = comment_tag.find(class_="comment-selected-answers")
    comment_en = comment_tag.find(class_="comment-content").get_text(
        separator="\n", strip=True
    )
    comment_jp = comment_en if comment_en.startswith(
        "http") else translator.translate(comment_en, dest="ja").text
    comment_jp = comment_jp.replace('en-us', 'ja-jp')
    comment["date"] = date.get_text()
    if badge:
        comment["badge"] = badge.get_text()
    if selected:
        comment["selected"] = selected.get_text()
    comment["comment_en"] = comment_en
    comment["comment_jp"] = comment_jp.replace('。', '。\n')
    comment["replays"] = []
    replies = comment_tag.find(class_="comment-replies")
    replay_list = replies.find_all(class_="comment-container", recursive=False)
    for replay in replay_list:
        comment["replays"].append(parse(replay, translator))
    return comment
