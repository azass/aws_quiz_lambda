import MeCab
import ctypes
import os
import re
from collections import defaultdict

import boto3
from boto3.dynamodb.conditions import Key

num_of_times_to_complete = 4

mecabdir = os.path.join(os.getcwd(), '/var/task/.mecab')
libmecab = ctypes.cdll.LoadLibrary(os.path.join(mecabdir, 'lib/libmecab.so'))


output_format_type = 'chasen'
dicdir = os.path.join(mecabdir, 'lib/mecab/dic/ipadic')
rcfile = os.path.join(mecabdir, 'etc/mecabrc')

tagger = MeCab.Tagger(
    '-O{} -d{} -r{}'.format(output_format_type, dicdir, rcfile))
p = re.compile('[a-zA-Z]+')

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    try:
        quest_id = event['quest_id']
        nouns = defaultdict(int)
        table = dynamodb.Table("Question")
        result = table.query(
            KeyConditionExpression=Key('quest_id').eq(quest_id)
        )
        if result["Count"] > 0:
            words = ""
            question = result["Items"][0]
            for question_item in question['question_items']:
                if 'text' in question_item:
                    words = words + question_item['text']
            for option in question['options']:
                if 'text' in option:
                    words = words + option['text'][2:]
            for explan_item in question['explanation']:
                if 'link' in explan_item:
                    if not explan_item['link'].startswith('TOPIC') and not explan_item['link'].endswith('DISCUSSION'):
                        words = words + explan_item['link']
                if 'text' in explan_item:
                    words = words + explan_item['text']

            noun_before = ""
            of_noun_before = ""
            node = tagger.parseToNode(words)
            while node:
                word = node.surface
                hinshi = node.feature.split(",")[0]
                if hinshi == '名詞':
                    if noun_before == '':
                        nouns[word] += 1
                        noun_before = word
                    else:
                        nouns[noun_before + word] += 1
                        noun_before += word
                    if p.fullmatch(word):
                        noun_before += ' '
                    if of_noun_before != '':
                        nouns[of_noun_before + word] += 1
                        of_noun_before += word
                        if p.fullmatch(word):
                            of_noun_before += ' '
                elif word == 'の':
                    if noun_before != '':
                        of_noun_before = noun_before + word
                        noun_before = ""
                else:
                    noun_before = ""
                    of_noun_before = ""

                node = node.next

            table = dynamodb.Table("Keyword")
            queryData = table.query(
                KeyConditionExpression=Key('quest_id').eq("COM")
            )
            items = queryData['Items']
            hide_words = []
            for keyword in items:
                hide_words.append(keyword['word'])

            table = dynamodb.Table("Keyword")
            queryData = table.query(
                KeyConditionExpression=Key('quest_id').eq(quest_id)
            )
            items = queryData['Items']
            now_keywords = []
            for keyword in items:
                if 'hide' in keyword:
                    hide_words.append(keyword['word'])
                else:
                    now_keywords.append(keyword['word'])

            keywords = []
            for noun in nouns:
                if noun.startswith('それぞれ'):
                    continue
                if noun.startswith('ための'):
                    continue
                if 'https://' in noun:
                    continue
                if 'AWS Black' in noun:
                    continue
                if noun not in hide_words:
                    keywords.append(
                        {"word": noun, "count": nouns[noun], "check_on": noun in now_keywords})
            return keywords

    except Exception as e:
        print("Error Exception.")
        print(type(e))
        print(e)
