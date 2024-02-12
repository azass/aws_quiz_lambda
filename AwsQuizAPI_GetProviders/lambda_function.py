import boto3
from boto3.dynamodb.conditions import Key, Attr
import traceback

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    try:
        providers = []
        scanData = dynamodb.Table("Provider").scan()
        providerData = scanData['Items']
        providerData.sort(key=lambda item: item['sort'])
        # scanData = dynamodb.Table("Exam").scan()
        # examData = scanData['Items']
        scanData = dynamodb.Table("Tag").scan()
        tagData = scanData['Items']
        for provider in providerData:
            print(provider['name'])
            exams = get_exams(provider['name'])
            # exams = list(
            #     filter(lambda exam: exam['provider'] == provider['name'], examData))
            exams.sort(key=lambda item: item['sort'])
            provider['exams'] = exams
            tags = list(
                filter(lambda tag: tag['provider'] == provider['name'], tagData))
            tags.sort(key=lambda item: item['tag_no'])
            provider['tags'] = tags
            providers.append(provider)
        print(providers)
        return providers
    except Exception as e:
        print("Error Exception.")
        print(type(e))
        print(e)
        print(f"Error Exception. {type(e)}: {e}")
        print(traceback.format_exc())


def get_exams(provider):
    tag_map = get_tag_map(provider)
    
    scan_params = {}
    scan_params['FilterExpression'] = Attr('provider').eq(provider)
    scanData = dynamodb.Table("Exam").scan(**scan_params)
    exams = scanData['Items']
    for exam in exams:
        queryData = dynamodb.Table("ReportItem").query(
            ProjectionExpression='tag_no, question_count',
            KeyConditionExpression=Key('exam_id').eq(exam['exam_id']),
            ScanIndexForward=True,
        )
        tags = queryData['Items']
        for tag in tags:
            tag['tag_name'] = tag_map[tag['tag_no']]
            tag['provider'] = provider
        exam['tags'] = tags
        
    return exams


def get_tag_map(provider):
    queryData = dynamodb.Table("Tag").query(
        ProjectionExpression="tag_no, tag_name",
        KeyConditionExpression=Key('provider').eq(provider),
    )
    tags = queryData['Items']
    tag_map = {}
    for tag in tags:
        tag_map[tag['tag_no']] = tag['tag_name']
    return tag_map
