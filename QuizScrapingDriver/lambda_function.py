from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from time import sleep
import boto3
import json
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
options = webdriver.ChromeOptions()
options.binary_location = "/opt/headless/python/bin/headless-chromium"
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--hide-scrollbars")
options.add_argument("--single-process")
options.add_argument("--ignore-certificate-errors")
options.add_argument("--window-size=880x996")
options.add_argument("--no-sandbox")
options.add_argument("--homedir=/tmp")

lambdaclient = boto3.client('lambda', 'ap-northeast-1')

def lambda_handler(event, context):
    browser = webdriver.Chrome(
        executable_path="/opt/headless/python/bin/chromedriver",
        options=options
    )
    try:
        URL = event['url']
        print(URL)
        # sleep(1)
        # URL = "https://www.examtopics.com/discussions/google/view/19550-exam-associate-cloud-engineer-topic-1-question-22-discussion/"
        # browser.set_page_load_timeout(30.0)
        # browser.set_script_timeout(30.0)
        browser.get(URL)
        
        # sleep(3)
        # print(browser.page_source)
        # quest = browser.find_element_by_class_name("question-body")
        # print(quest)
        # for q in enumerate(quest):
        #     print(q)
            # print(q.find_element_by_tag_name("p"))
            # q_en = q.find_element_by_tag_name("p").text()
    
        params = {}
        params["quest_id"] = event["quest_id"]
        params['data'] = browser.page_source

        dynamodb = boto3.resource('dynamodb')
        dynamodb.Table("Comments").update_item(
            Key={
                'quest_id': event["quest_id"]
            },
            UpdateExpression="SET src = :val4",
            ExpressionAttributeValues={
                ':val4': params['data']
            }
        )

        resp = lambdaclient.invoke(
            FunctionName='QuizScraping',
            InvocationType='RequestResponse',
            Payload=json.dumps(params)
        )
        payload = resp['Payload'].read()
        # print(payload)
        payload_str = payload.decode('utf-8')
        # logger.info("lambda invoke resp['Payload']: {}".format(payload_str))
        # title = browser.title
        browser.close()
        return payload
    except Exception as e:
        browser.close()
