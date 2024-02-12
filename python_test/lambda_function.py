import json
from datetime import datetime, timedelta, timezone


def lambda_handler(event, context):
    # print()
    today = None
    test = False
    if test:
        today = datetime.now()
    if today:
        print("OK")
    print(today)

    num_of_times_to_complete = 4
    exam_count = 150
    point = 153
    level = point * 10 / num_of_times_to_complete / exam_count
    print(level)
    print(int(level))
