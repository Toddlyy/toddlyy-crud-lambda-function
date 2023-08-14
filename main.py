# https://github.com/ArthurGartner/tutorials/blob/e81dd251fc65affad049f57d5159ec23e05282a6/platforms/aws/lambda/felixyu/lambdacrud/lamnda_function.py
# https://www.youtube.com/watch?v=9eHh946qTIk
# Ctrl+Shift+Alt+L

import json
import boto3
from custom_encoder import CustomEncoder
import logging
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

logger = logging.getLogger()
logger.setLevel(logging.INFO)

user_table_name = "User"
daycare_table_name = "Daycares"
booking_table_name = "Bookings"

dynamodb = boto3.resource('dynamodb', region_name=str("ap-south-1"))

userPath = "/user"
daycarePath = "/daycare"
bookingPath = "/booking"

getMethod = "GET"
postMethod = "POST"
patchMethod = "PATCH"
deleteMethod = "DELETE"

user_table = dynamodb.Table(user_table_name)
daycare_table = dynamodb.Table(daycare_table_name)
booking_table = dynamodb.Table(booking_table_name)


def lambda_handler(event, context):
    # print(event)
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

    elif path == bookingPath:
        if httpMethod == getMethod:
            response = display_booking(event['queryStringParameters']['username'])
        elif httpMethod == postMethod:
            response = create_booking(json.loads(event['body'], parse_float=Decimal))
        else:
            response = buildResponse(404, 'Booking URL Not Found')

    else:
        response = buildResponse(404, 'URL Not Found')

    return response


def get_user(username):
    try:
        # print("Displaying user info")

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
        # print("Displaying daycares")
        # Change scan to query when we want to scale
        response = daycare_table.scan(ProjectionExpression="daycareID, #daycare_name, image, #region_name",
                                      ExpressionAttributeNames={'#daycare_name': 'name', '#region_name': 'region'})
        # print(json.dumps(response))
        if 'Items' in response:
            return buildResponse(200, response['Items'])
        else:
            return buildResponse(404, {'Message': 'Daycares not found'})

    except:
        logger.exception('Exception thrown on displaying daycares')


def get_daycare_info(daycare_id):
    # print("Getting daycare info for daycare " + daycare_id)
    try:
        response = daycare_table.query(KeyConditionExpression=Key('daycareID').eq(daycare_id))
        
        ##### tO GET URL OF IMAGES ####
        s3_client = boto3.client('s3')
        url_prefix = 'https://toddlyybucket.s3.ap-south-1.amazonaws.com/'

        images_list = []

        for key in s3_client.list_objects(Bucket='toddlyybucket',Prefix=daycare_id)['Contents']:
            images_list.append(url_prefix + key['Key'])
   
        images_list.pop(0)
        
        ################################
                
        if 'Items' in response:
            response['Items'].append(images_list)
            return buildResponse(200, response['Items'])
        else:
            return buildResponse(404, {'Message': 'Daycare %s not found' % daycare_id})
    except:
        logger.exception('Exception thrown on getting daycare info')


def create_user(requestBody):
    try:
        # print("Adding a user")
        # print("requestBody = " + requestBody["username"])

        response = user_table.get_item(Key={
            'username': requestBody["username"]
        })

        if 'Item' in response:
            # print("User exists")
            update_user(requestBody["username"], "firstName", requestBody["firstName"])
            update_user(requestBody["username"], "lastName", requestBody["lastName"])
            body = {
                'Operation': 'LOGIN USER',
                'Message': 'SUCCESS',
                'Item': requestBody
            }


        else:
            # print("User doesn't exist")
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


#BOOKING ID = USERNAME OF USER FOR NOW(Will change while scaling up)
def create_booking(requestBody):
    try:
        # print("Creating new booking for user %s" % requestBody["bookingID"])

        booking_table.put_item(
            Item=requestBody)

        body = {
            'Operation': 'CREATE NEW BOOKING',
            'Message': 'SUCCESS',
            'Item': requestBody
        }

        return buildResponse(200, body)

    except:
        logger.exception('Exception thrown on creating booking')


def display_booking(username):
    cutOffTime = (datetime.now(ZoneInfo('Asia/Kolkata')) + timedelta(hours=1)).isoformat()[:26]
    # print("Getting booking info for user " + username)
    try:
        response = booking_table.query(KeyConditionExpression=Key('bookingID').eq(username)
                                                              & Key("endTime").gt(cutOffTime))
        if 'Items' in response:
            return buildResponse(200, response['Items'])
        else:
            return buildResponse(404, {'Message': 'Booking not found for user %s' % username})
    except:
        logger.exception('Exception thrown on getting daycare info')


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
