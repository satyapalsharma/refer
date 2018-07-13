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

# db_settings = {
#     "DB_HOST": "localhost",
#     "DB_USER": "fabDev",
#     "DB_PASS": "Fab@1962",
#     "DB_NAME": "FabHotels"
# }

# db_read_config = {
#     "DB_HOST": "localhost",
#     "DB_USER": "fabDev",
#     "DB_PASS": "Fab@1962",
#     "DB_NAME": "FabHotels"
# }

# db_write_config = {
#     "DB_HOST": "localhost",
#     "DB_USER": "fabDev",
#     "DB_PASS": "Fab@1962",
#     "DB_NAME": "FabHotels"
# }

db_read_config = {
    "DB_HOST": "fabuser-microservice-read.cwwl28odsw8p.ap-south-1.rds.amazonaws.com",
    "DB_USER": "refferral_read",
    "DB_PASS": "mSq6Tr9ZYNw6ryqB",
    "DB_NAME": "refferral_microservice"
}

db_write_config = {
    "DB_HOST": "fabuser-microservice.cwwl28odsw8p.ap-south-1.rds.amazonaws.com",
    "DB_USER": "refferral_write",
    "DB_PASS": "ushRtSTvEg5nytDJ",
    "DB_NAME": "refferral_microservice"
}

# POINTS_SERVICE_URL = 'http://13.127.243.15:8080'

# POINTS_SERVICE_URL = 'http://172.31.7.216:8080'

POINTS_SERVICE_URL = 'http://internal-points-load-balancer-1148785792.ap-south-1.elb.amazonaws.com'

POINTS_PLAN_OBJECT = {}

def get_points_plan_object():
    if POINTS_PLAN_OBJECT:
        return POINTS_PLAN_OBJECT

    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    with dbRead.cursor() as cursor:
        sql = "SELECT points, event, expiry FROM `referral_pointsplan`"
        cursor.execute(sql)
        result = cursor.fetchall()
        ret = {}
        for row in result:
            ret[row['event']] = row
    dbRead.close()
    return ret

POINTS_PLAN_OBJECT = get_points_plan_object()


maxBonusCountByReferrer = POINTS_PLAN_OBJECT.get('MaxReferralLimit', {}).get('points', 0)

def convert_transaction_type(payload):
    headers ={'content-type': "application/json"}
    url = POINTS_SERVICE_URL+ "/fabpoints/admin/pointsservice/referral/realisedcredit"
    response = requests.request('POST', url, data=str(json.dumps(payload)),headers=headers)
    responseJson = response.json()
    responseStatus = responseJson.get('status', False)

    dbWrite = pymysql.connect(host=db_write_config["DB_HOST"], user=db_write_config["DB_USER"], password=db_write_config["DB_PASS"], db=db_write_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    if responseStatus:
        responseData = responseJson.get('data', False)
        print(responseJson)
        print(responseData)
        for transactionId in responseData:
            transactionStatus = responseData.get(transactionId, False)
            if transactionStatus:
                print(transactionId, transactionStatus)
                with dbWrite.cursor() as writeCursor:
                    sql = "UPDATE `transactions` SET TRANSACTION_TYPE=1 where ACTIVE=1 AND TRANSACTION_ID='{}'".format(transactionId)
                    print(sql)
                    writeCursor.execute(sql)
    dbWrite.close()


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
    consumer = KafkaConsumer('user',bootstrap_servers=['10.0.1.220:9092', '10.0.1.81:9092'], auto_offset_reset='earliest')
    print(consumer)
    while(True):
        print('inside loop')
        if consumer:
            for msg in consumer:
                msgData = msg.value.decode("utf-8")
                jsonData = json.loads(msgData)
                print ('got data', jsonData)

                userMobileStatusKafka = jsonData.get('isMobileVerified', False)
                userUuidKafka = jsonData.get('userId', '')

                if userMobileStatusKafka:

                    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
                    dbWrite = pymysql.connect(host=db_write_config["DB_HOST"], user=db_write_config["DB_USER"], password=db_write_config["DB_PASS"], db=db_write_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

                    with dbRead.cursor() as readCursor:
                        sql = "SELECT ID, UUID, MOBILE_VERIFIED, FIRST_CHECKOUT FROM `referral_mapping` where ACTIVE=1 AND UUID='{0}'".format(userUuidKafka)
                        readCursor.execute(sql)
                        result = readCursor.fetchone()

                        if result:
                            userMobileStatusDb = bool(ord(result.get('MOBILE_VERIFIED', False)))
                            userReferralMappingId = result.get('ID', False)

                            if userMobileStatusKafka == userMobileStatusDb:
                                print('No need for status update')
                            else:
                                sql = "SELECT ID, UUID FROM `referral_mapping` where ACTIVE=1 AND REFERRAR_UUID='{0}' AND FIRST_CHECKOUT=1 LIMIT 10".format(userUuidKafka)
                                print(sql)
                                readCursor.execute(sql)
                                referedUserResult = readCursor.fetchall()

                                idList = []
                                for referedUserDetails in referedUserResult:
                                    idList.append(referedUserDetails.get('ID', ''))
                                
                                print(idList)
                                transactionIdList = []

                                if len(idList) > 0:
                                    sql = "SELECT TRANSACTION_ID FROM `transactions` where ACTIVE=1 AND TRANSACTION_TYPE=0 AND REFERRAL_MAPPING_ID in ({0}) AND AFFECTED_USER_UUID='{1}' AND DISCOUNT_TYPE=1 LIMIT 10".format(",".join(map(str, idList)), userUuidKafka)
                                    print(sql)
                                    readCursor.execute(sql)
                                    result = readCursor.fetchall()

                                    for transactionId in result:
                                        transactionIdList.append(transactionId.get('TRANSACTION_ID', ''))

                                print(transactionIdList)

                                sql = "SELECT TRANSACTION_ID FROM `transactions` where  ACTIVE=1 AND TRANSACTION_TYPE=0 AND REFERRAL_MAPPING_ID = '{0}' AND AFFECTED_USER_UUID='{1}' AND DISCOUNT_TYPE=0 LIMIT 10".format(userReferralMappingId, userUuidKafka)
                                print(sql)
                                readCursor.execute(sql)
                                result = readCursor.fetchone()
                                transactionIdList.append(result.get('TRANSACTION_ID', ''))

                                print(transactionIdList)

                                convert_transaction_type(transactionIdList)

                                with dbWrite.cursor() as writeCursor:
                                    sql = "UPDATE `referral_mapping` SET MOBILE_VERIFIED=1 where ID='{0}'".format(userReferralMappingId)
                                    print(sql)
                                    writeCursor.execute(sql)
                    dbRead.close()
                    dbWrite.close()


kafkaCall()