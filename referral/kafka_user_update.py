import json
import requests
from threading import Thread
from kafka import KafkaConsumer,logging
import pymysql
import pymysql.cursors


# db_settings = {
#     "DB_HOST": "localhost",
#     "DB_USER": "root",
#     "DB_PASS": "Yamyanyo2??",
#     "DB_NAME": "referral"
# }

db_settings = {
    "DB_HOST": "localhost",
    "DB_USER": "fabDev",
    "DB_PASS": "Fab@1962",
    "DB_NAME": "FabHotels"
}

# POINTS_SERVICE_URL = 'http://13.127.243.15:8080'

POINTS_SERVICE_URL = 'http://172.31.7.216:8080'


POINTS_PLAN_OBJECT = {}

def get_points_plan_object():
    if POINTS_PLAN_OBJECT:
        return POINTS_PLAN_OBJECT
    with dbRead.cursor() as cursor:
        sql = "SELECT points, event, expiry FROM `referral_pointsplan`"
        cursor.execute(sql)
        result = cursor.fetchall()
        ret = {}
        for row in result:
            ret[row['event']] = row
        return ret

POINTS_PLAN_OBJECT = get_points_plan_object()


maxBonusCountByReferrer = POINTS_PLAN_OBJECT.get('MaxReferralLimit', {}).get('points', 0)


dbRead = pymysql.connect(host=db_settings["DB_HOST"], user=db_settings["DB_USER"], password=db_settings["DB_PASS"], db=db_settings["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
dbWrite = pymysql.connect(host=db_settings["DB_HOST"], user=db_settings["DB_USER"], password=db_settings["DB_PASS"], db=db_settings["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def convert_transaction_type(payload):
    headers ={'content-type': "application/json"}
    url = POINTS_SERVICE_URL+ "/fabpoints/admin/pointsservice/referral/realisedcredit"
    response = requests.request('POST', url, data=str(json.dumps(payload)),headers=headers)
    responseJson = response.json()
    responseStatus = responseJson.get('status', False)
    if responseStatus:
        responseData = responseJson.get('data', False)
        print(responseJson)
        print(responseData)
        for transactionId in responseData:
            transactionStatus = responseData.get(transactionId, False)
            if transactionStatus:
                print(transactionId, transactionStatus)
                with dbWrite.cursor() as writeCursor:
                    sql = "UPDATE `transactions` SET TRANSACTION_TYPE=1 where TRANSACTION_ID='{}'".format(transactionId)
                    print(sql)
                    writeCursor.execute(sql)


def kafkaCall():


    """
        Few cases to take care of while making credits realised:

        1. It must be kafka user's first booking.
        2. The status of the booking should be CHECKOUT as credits for the user who has referred him
            will be realised only when kafka user has checkout.
        3. Referrer corressponding to Kafka user must have verified mobile number.
            a) if he doesn't have a verified mobile, the state of the corresoding transaction
                will be marked as MOBILE_VERIFICATION_PENDING.
        4. Transaction for the Referrer will be realised only if the MAX_LIMIT is not reached
    """
    print ('inside kafka call')

    #creating kafka consumer
    # consumer = KafkaConsumer('bookingpoints',bootstrap_servers='13.127.227.239:9092', auto_offset_reset='earliest')
    consumer = KafkaConsumer('user',bootstrap_servers='172.31.21.241:9092', auto_offset_reset='earliest')
    print(consumer)
    while(True):
        print('inside loop')
        if consumer:
            for msg in consumer:
                msg = eval(msg.value)
                print ('data',msg)
                print ('got data')