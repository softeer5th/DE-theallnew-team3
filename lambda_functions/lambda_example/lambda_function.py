import json
import boto3
import requests


def lambda_handler(event, context):
    print("hello~")
    print("ok")
    print("hi")

    return {"statusCode": 200, "result": "hello from lambda~", "event": event}
