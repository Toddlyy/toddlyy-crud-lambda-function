# https://github.com/ArthurGartner/tutorials/blob/e81dd251fc65affad049f57d5159ec23e05282a6/platforms/aws/lambda/felixyu/lambdacrud/lamnda_function.py
# https://www.youtube.com/watch?v=9eHh946qTIk

import json
import boto3
from custom_encoder import CustomEncoder
import logging
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

user_table_name = "User"
daycare_table_name = "Daycares"
dynamodb = boto3.resource('dynamodb', region_name=str("ap-south-1"))

userPath = "/user"
daycarePath = "/daycare"

getMethod = "GET"
postMethod = "POST"
patchMethod = "PATCH"
deleteMethod = "DELETE"

user_table = dynamodb.Table(user_table_name)
daycare_table = dynamodb.Table(daycare_table_name)


def lambda_handler(event, context):
    print(event)
    httpMethod = event['httpMethod']
    path = event['path']

    if path == userPath:
        if httpMethod == postMethod:
            response = create_user(json.loads(event['body']))
        elif httpMethod == getMethod:
            response = get_user(event['queryStringParameters']['username'])
        elif httpMethod == patchMethod:
            requestBody = json.loads(event['body'])
            response = update_user(requestBody['username'],
                                  requestBody['updateKey'], requestBody['updateValue'])
        else:
            response = buildResponse(404, 'User URL Not Found')

    elif path == daycarePath:
        if httpMethod == getMethod and event['queryStringParameters'] is None:
            response = display_daycares()

        elif httpMethod == getMethod and event['queryStringParameters'] is not None:
            response = get_daycare_info(event['queryStringParameters']['daycareID'])

        else:
            response = buildResponse(404, 'Daycare URL Not Found')

    else:
        response = buildResponse(404, 'URL Not Found')

    return response


def get_user(username):
    try:
        print("Displaying user info")

        response = user_table.get_item(Key={
            'username': username
        })

        if 'Item' in response:
            return buildResponse(200, response['Item'])
        else:
            return buildResponse(404, {'Message': 'Username %s not found' % username})
    except:
        logger.exception('Exception thrown on retrieving user')


def display_daycares():
    try:
        print("Displaying daycares")
        #Change scan to query when we want to scale
        response = daycare_table.scan(ProjectionExpression="daycareID, #daycare_name, image",
                                       ExpressionAttributeNames={'#daycare_name': 'name'})
        print(json.dumps(response))
        if 'Items' in response:
            return buildResponse(200, response['Items'])
        else:
            return buildResponse(404, {'Message': 'Daycares not found'})

    except:
        logger.exception('Exception thrown on displaying daycares')

def get_daycare_info(daycare_id):
    print("Getting daycare info for daycare " + daycare_id)
    try:
        response = daycare_table.query(KeyConditionExpression=Key('daycareID').eq(daycare_id))
        if 'Items' in response:
            return buildResponse(200, response['Items'])
        else:
            return buildResponse(404, {'Message': 'Daycare %s not found' % daycare_id})
    except:
        logger.exception('Exception thrown on getting daycare info')


def create_user(requestBody):
    try:
        print("Adding a user")
        print("requestBody = " + requestBody["username"])

        response = user_table.get_item(Key={
            'username': requestBody["username"]
        })

        if 'Item' in response:
            print("User exists")
            update_user(requestBody["username"], "firstName", requestBody["firstName"])
            update_user(requestBody["username"], "lastName", requestBody["lastName"])
            body = {
                'Operation': 'LOGIN USER',
                'Message': 'SUCCESS',
                'Item': requestBody
            }


        else:
            print("User doesn't exist")
            table_response = user_table.put_item(
                Item=requestBody)
            body = {
                'Operation': 'CREATE NEW USER',
                'Message': 'SUCCESS',
                'Item': requestBody
            }

        return buildResponse(200, body)

    except:
        logger.exception('Exception thrown on creating user')


def update_user(username, updateKey, updateValue):
    try:
        response = user_table.update_item(
            Key={
                'username': username
            },
            UpdateExpression='set %s = :value' % updateKey,
            ExpressionAttributeValues={
                ':value': updateValue
            }
        )
        body = {
            'Operation': 'UPDATE',
            'Message': 'SUCCESS',
            'UpdatedAttributes': response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Error updating user')


def buildResponse(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }

    if body is not None:
        response['body'] = json.dumps(body, cls=CustomEncoder)

    return response