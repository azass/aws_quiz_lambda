import boto3


def lambda_handler(event, context):
    try:
        provider = event['provider']
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table("Exam")
        print("start scan")
        scanData = table.scan()
        print("end scan")
        items = scanData['Items']
        print("start extract")
        items = list(filter(lambda exam: exam['provider'] == provider, items))
        print("end extract")
        print("start sort")
        items.sort
        print("end sort")
        return items
        
    except Exception as e:
        print("Error Exception.")
        print(type(e))
        print(e)
