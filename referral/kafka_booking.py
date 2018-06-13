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

dbRead = pymysql.connect(host=db_settings["DB_HOST"], user=db_settings["DB_USER"], password=db_settings["DB_PASS"], db=db_settings["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
dbWrite = pymysql.connect(host=db_settings["DB_HOST"], user=db_settings["DB_USER"], password=db_settings["DB_PASS"], db=db_settings["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

def convert_transaction_type(payload):
    payload = [
        "298e265a-3192-41e8-87c5-b3b1b029baab"
    ]
    headers ={'content-type': "application/json"}
    url = POINTS_SERVICE_URL+ "/fabpoints/admin/pointsservice/referral/realisedcredit"
    response = requests.request('POST', url, data=str(json.dumps(payload)),headers=headers)
    responseJson = response.json()
    responseStatus = responseJson.get('status', False)
    if responseStatus:
        # responseData = responseJson.get('data', False)
        responseData = {'38de4723-11b6-4179-b2df-1f2d02d104b1': True, 'af71d65d-9025-41cf-9505-fc771eeb37f6': True}
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
    consumer = KafkaConsumer('bookingpoints',bootstrap_servers='172.31.21.241:9092', auto_offset_reset='earliest')
    print(consumer)
    while(True):
        print('inside loop')
        if consumer:
            for msg in consumer:
                msg = eval(msg.value)
                # print ('data',data)
                # print ('got data')
                action = msg.get('guestStatus', '')
                uuid = msg.get('userId', '')
                if action=='CHECKOUT':
                    with dbRead.cursor() as readCursor:
                        sql = "SELECT ID, UUID, REFERRAR_UUID FROM `referral_mapping` where UUID='{0}'".format(uuid)
                        readCursor.execute(sql)
                        result = readCursor.fetchone()
                        if result:
                            referrarUuid = result.get('REFERRAR_UUID', '')
                            userId = result.get('ID', 0)
                            sql = "SELECT UUID, MOBILE_VERIFIED FROM `referral_mapping` where UUID='{0}'".format(referrarUuid)
                            readCursor.execute(sql)
                            referrarResult = readCursor.fetchone()
                            referrarMobileVerified = referrarResult.get('MOBILE_VERIFIED', False)
                            if referrarMobileVerified:
                                sql = "SELECT TRANSACTION_ID FROM `transactions` where AFFECTED_USER_UUID='{0}' AND DISCOUNT_TYPE=1 AND TRANSACTION_TYPE=0  ".format(referrarUuid)
                                readCursor.execute(sql)
                                result = readCursor.fetchone()
                                if result:
                                    payload = [result.get('TRANSACTION_ID')]
                                    convert_transaction_type(payload)
                            with dbWrite.cursor() as writeCursor:
                                sql = "UPDATE `referral_mapping` SET FIRST_CHECKOUT=1 where ID='{0}'".format(userId)
                                print(sql)
                                writeCursor.execute(sql)
                                
                        else:
                            print('this uuid is not in the referral system')
    consumer.close()
    
                
            


kafkaCall()

# def run():
#     run_thread = Thread(target=kafkaCall())
#     run_thread.daemon = True
#     run_thread.start()
#     time.sleep(20)