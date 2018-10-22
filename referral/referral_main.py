from sanic import Sanic
from sanic.response import json
import string
from datetime import datetime
import random
import pymysql
import pymysql.cursors
import requests
import json as pyjson
import re
from kafka import KafkaProducer

KAFKA_TOPIC = 'sms'
# KAFKA_BROKERS = ['localhost:9092']
KAFKA_BROKERS = ['10.0.1.220:9092', '10.0.1.81:9092']

# POINTS_SERVICE_URL = 'http://13.127.243.15:8080'

# POINTS_SERVICE_URL = 'http://localhost:8070'

POINTS_SERVICE_URL = 'http://internal-points-load-balancer-1148785792.ap-south-1.elb.amazonaws.com:8020'

app = Sanic()

# db_read_config = {
#     "DB_HOST": "127.0.0.1",
#     "DB_USER": "root",
#     "DB_PASS": "Yamyanyo2??",
#     "DB_NAME": "referral_microservice"
# }

# db_write_config = {
#     "DB_HOST": "127.0.0.1",
#     "DB_USER": "root",
#     "DB_PASS": "Yamyanyo2??",
#     "DB_NAME": "referral_microservice"
# }

# db_read_config = {
#     "DB_HOST": "localhost",
#     "DB_USER": "root",
#     "DB_PASS": "3p7G(kyJ?yN)~22X",
#     "DB_NAME": "referral_microservice"
# }

# db_write_config = {
#     "DB_HOST": "localhost",
#     "DB_USER": "root",
#     "DB_PASS": "3p7G(kyJ?yN)~22X",
#     "DB_NAME": "referral_microservice"
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

TRANSACTION_TYPE = {
    "UNREALISEDCREDIT": 0,
    "CREDIT": 1
}

CREDIT_TYPE = {
    "Referral Points": 1,
    "Instant Referral Discount": 0

}

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

def commit_transaction(responseData, referralMappingId, uuid, creditType):
    dbWrite = pymysql.connect(host=db_write_config["DB_HOST"], user=db_write_config["DB_USER"], password=db_write_config["DB_PASS"], db=db_write_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    
    print(responseData)
    transactionId = responseData.get('transactionId', '')
    transactionType = TRANSACTION_TYPE.get(responseData.get('transactionType'))
    
    creditType = CREDIT_TYPE.get(creditType)
    try:
        with dbWrite.cursor() as writeCursor:
            sql = "INSERT INTO transactions (TRANSACTION_ID, TRANSACTION_TYPE, REFERRAL_MAPPING_ID, AFFECTED_USER_UUID, DISCOUNT_TYPE) VALUES ('{0}', {1}, {2}, '{3}', {4})".format(transactionId, transactionType, referralMappingId, uuid, creditType)
            print(sql)
            writeCursor.execute(sql)
            result = writeCursor.fetchone()
    except:
        print ('Some error occured in commiting transaction. Probabally a duplicate entry')
    finally:
        dbWrite.close()

#Function to add credits in user's account
def add_credit(uuid='', pointsType='Referral Points', sourceId=123, propertyId=645, creditType='UNREALISED'):
    if creditType=='UNREALISED':
        url = POINTS_SERVICE_URL + "/fabpoints/admin/pointsservice/referral/credit"
    elif creditType=='REALISED':
        url = POINTS_SERVICE_URL + "/fabpoints/admin/pointsservice/referral/instantcredit"

    data={}
    data["userId"] = uuid
    data["points"] = POINTS_PLAN_OBJECT.get(pointsType, {}).get('points')
    data["expireDays"] = POINTS_PLAN_OBJECT.get(pointsType, {}).get('expiry')
    data["eventDateTime"] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    data["eventSourceId"] = str(sourceId)
    data['propertyId'] = propertyId
    
    headers ={'content-type': "application/json",}
    response = requests.request('POST', url, data=str(data),headers=headers)
    jsonResponse = response.json()
    
    responseStatus = jsonResponse.get('status', False)

    if responseStatus:
        commit_transaction(jsonResponse.get('data'), sourceId, uuid, pointsType)
    return jsonResponse

def deactivate_transaction(responseData):
    dbWrite = pymysql.connect(host=db_write_config["DB_HOST"], user=db_write_config["DB_USER"], password=db_write_config["DB_PASS"], db=db_write_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    
    print(responseData)

    transactionId = responseData.get('transactionId', '')
    if transactionId:
        try:
            with dbWrite.cursor() as writeCursor:
                sql = "UPDATE transactions SET ACTIVE=0 WHERE TRANSACTION_ID=%s"
                print(sql)
                writeCursor.execute(sql, transactionId)
                result = writeCursor.fetchone()
        except Exception as e: 
            print(e)
        finally:
            dbWrite.close()


def revoke_credit(transactionId=''):
    url = POINTS_SERVICE_URL + "/fabpoints/admin/pointsservice/transaction/invalidate/{0}".format(transactionId)

    headers ={'content-type': "application/json",}
    response = requests.request('POST', url, headers=headers)
    jsonResponse = response.json()
    print(jsonResponse)

    responseStatus = jsonResponse.get('status', False)

    if responseStatus:
        deactivate_transaction(jsonResponse.get('data'))
    return jsonResponse


# function to generate referral code
def code_generator(size=6, default='', chars=string.ascii_uppercase.replace("O","") +string.digits.replace("0","")):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    
    while True:
        if default:
            unique_code = default.upper()[0:10]
            default = ''
        else:
            unique_code = ''.join(random.choice(chars) for _ in range(size))
        try:
            with dbRead.cursor() as cursor:
                sql = "SELECT CODE FROM `referral_mapping` WHERE ACTIVE=1 AND `CODE`=%s"
                cursor.execute(sql, unique_code)
                result = cursor.fetchone()
                if not result:
                    break
        except Exception as e: 
            print(e)

    dbRead.close()  
    return unique_code

def should_user_get_referral_bonus(referrerUuid=''):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    
    try:
        with dbRead.cursor() as readCursor:
            sql = "SELECT count(*) as recivedBonusCountByReferrer FROM `transactions` where ACTIVE=1 AND AFFECTED_USER_UUID='{0}' AND DISCOUNT_TYPE=1 AND TRANSACTION_TYPE=1  ".format(referrerUuid)
            readCursor.execute(sql)
            bonusCount = readCursor.fetchone()
            recivedBonusCountByReferrer = bonusCount.get('recivedBonusCountByReferrer', 0)
            print("User {0} has recived referral bonus for {1} out of max {2} allowed".format(referrerUuid, recivedBonusCountByReferrer, maxBonusCountByReferrer))
    except Exception as e: 
        print(e)
    finally:
        dbRead.close()
    return (recivedBonusCountByReferrer < maxBonusCountByReferrer)

def create_response(success=False, data={}, message=''):
    ret = {
        "success": success,
        "data": data,
        "message": message
    }
    return ret

@app.route("/health_check", methods=['POST', 'GET'])
async def showHealth(request):
    return json(create_response(True))

@app.route("/ref/code", methods=['POST'])
async def getOrGeneratCode(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    dbWrite = pymysql.connect(host=db_write_config["DB_HOST"], user=db_write_config["DB_USER"], password=db_write_config["DB_PASS"], db=db_write_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    name = request.json.get('name', '')
    uuid = request.json.get('uuid', '')
    mobileVerified = request.json.get('mobileVerified', 1)
    print(request.json)

    if uuid:
        with dbRead.cursor() as cursor:
            sql = "SELECT CODE, MOBILE_VERIFIED, JOINING_BONUS, REFERRAL_BONUS FROM `referral_mapping` WHERE ACTIVE=1 AND `UUID`=%s"
            cursor.execute(sql, uuid)
            result = cursor.fetchone()
            if result:
                ret = {}
                ret['code'] = result['CODE']
                ret['bonus_received'] = bool(ord(result['REFERRAL_BONUS']))
                ret['mobile_verified'] = bool(ord(result['MOBILE_VERIFIED']))
                ret['instant_discount_received'] = bool(ord(result['JOINING_BONUS']))
                return json(create_response(True, ret))

        code = code_generator(default=name)

        with dbWrite.cursor() as readCursor:
            try:
                sql = "INSERT INTO `referral_mapping` (UUID, CODE, MOBILE_VERIFIED) VALUES ('{0}', '{1}', {2})".format(uuid, code, mobileVerified)
                print(sql)
                readCursor.execute(sql)
            except :
                print('Errpr:')
                return json(create_response())
            
        ret = {}
        ret['code'] = code
        ret['bonus_received'] = False
        ret['mobile_verified'] = False
        ret['instant_discount_received'] = False
        return json(create_response(True, ret))
    else:
        return json(create_response(message='UUID not provided'))

@app.route("/ref/count", methods=['POST'])
async def refCount(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    
    uuid = request.json.get('uuid', '')

    if uuid:
        with dbRead.cursor() as cursor:
            sql = "SELECT count(*) as referred_count FROM `referral_mapping` WHERE ACTIVE=1 AND `REFERRAR_UUID`=%s"
            cursor.execute(sql, uuid)
            result = cursor.fetchone()
            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='No data found'))
    else:
        return json(create_response(message='UUID not provided'))

@app.route("/points/plan", methods=['GET'])
async def pointsPlan(request):
    # with dbRead.cursor() as cursor:
    #     sql = "SELECT `points`, `event`, `expiry` FROM `referral_pointsplan`"
    #     cursor.execute(sql)
    #     result = cursor.fetchall()
    return json(create_response(True, POINTS_PLAN_OBJECT))

@app.route("/get/uuid", methods=['POST'])
async def getUuidFromReferralCode(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    code = request.json.get('code', '')

    if code:
        with dbRead.cursor() as cursor:
            sql = "SELECT UUID FROM `referral_mapping` where ACTIVE=1 AND CODE=%s"
            cursor.execute(sql, code)
            result = cursor.fetchone()
            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='referral code not found'))
    else:
        return json(create_response(message='referral code not provided'))

@app.route("/get/referrerCode", methods=['POST'])
async def getReferrerCode(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    uuid = request.json.get('uuid', '')

    if uuid:
        with dbRead.cursor() as readCursor:
            sql = "SELECT REFERRAR_UUID FROM `referral_mapping` where UUID=%s"
            print(sql)
            readCursor.execute(sql, uuid)
            result = readCursor.fetchone()
            if result:
                referrerUuid = result.get('REFERRAR_UUID', False)
                print('referrerUuid:',referrerUuid)
                if referrerUuid:
                    sql = "SELECT CODE FROM `referral_mapping` where UUID=%s"
                    print(sql)
                    readCursor.execute(sql, referrerUuid)
                    referrerResult = readCursor.fetchone()
                    print("DATA:", referrerResult)
                    if referrerResult:
                        return json(create_response(True, referrerResult))
                    else:
                        return json(create_response(message='Sorry no code found'))
                else:
                    return json(create_response(message='This user wasn\'t referred by anyone'))
            else:
                return json(create_response(message='Sorry can\'t find the provided uuid in the database'))
    else:
        return json(create_response(message='UUID not provided'))

    print(request.json)

@app.route("/get/code", methods=['POST'])
async def getReferralCodeFromUuid(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    uuid = request.json.get('uuid', '')

    if uuid:
        with dbRead.cursor() as cursor:
            if type(uuid) is list:
                sql = "SELECT CODE FROM `referral_mapping` where ACTIVE=1 AND UUID in ('{0}')".format("', '".join(str(ids) for ids in uuid))
                print(sql)
                cursor.execute(sql)
                result = cursor.fetchall()
            elif type(uuid) is str:
                sql = "SELECT CODE FROM `referral_mapping` where ACTIVE=1 AND UUID='{0}'".format(uuid)
                print(sql)
                cursor.execute(sql)
                result = cursor.fetchone()
            else:
                return json(create_response(message='I can not understand what you are trying to fetch'))

            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='uuid not found'))
    else:
        return json(create_response(message='uuid not provided'))


@app.route("/get/event_detail", methods=['POST'])
async def getActionDetailsFromMappingId(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    print(request.json)
    idList = request.json.get('idList', '')

    if idList:
        with dbRead.cursor() as cursor:
            if type(idList) is list:
                sql = "SELECT ID, UUID, REFERRAR_UUID, MOBILE_VERIFIED, FIRST_CHECKOUT, CODE FROM `referral_mapping` where ID in ({0})".format(', '.join(str(ids) for ids in idList))
                cursor.execute(sql)
                result = cursor.fetchall()
            elif type(idList) is int:
                sql = "SELECT ID, UUID, REFERRAR_UUID, MOBILE_VERIFIED, FIRST_CHECKOUT, CODE FROM `referral_mapping` where ID = ({0})".format(idList)
                cursor.execute(sql)
                result = cursor.fetchone()
            else:
                return json(create_response(message='Sorry I can\'t understand the data you requested'))

            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='details not found for provided IDs'))
    else:
        return json(create_response(message='Id list not provided'))

@app.route("/get/transaction_detail", methods=['POST'])
async def getTransactionDetailsFromTransaction(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    print(request.json)
    transactions = request.json.get('transactions', '')

    if transactions:
        with dbRead.cursor() as cursor:
            if type(transactions) is list:
                sql = "SELECT TRANSACTION_ID, TRANSACTION_TYPE, EXTRA_META_DATA FROM `transactions` where TRANSACTION_ID in ('{0}')".format("', '".join(str(ids) for ids in transactions))
                cursor.execute(sql)
                result = cursor.fetchall()
            elif type(transactions) is str:
                sql = "SELECT TRANSACTION_ID, TRANSACTION_TYPE, EXTRA_META_DATA FROM `transactions` where TRANSACTION_ID = ('{0}')".format(transactions)
                cursor.execute(sql)
                result = cursor.fetchone()
            else:
                return json(create_response(message='Sorry I can\'t understand the data you requested'))

            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='details not found for provided trasactions'))
    else:
        return json(create_response(message='transaction list not provided'))


@app.route("/check/code", methods=['POST'])
async def isValidReferralCode(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    code = request.json.get('code', '')

    if code:
        with dbRead.cursor() as cursor:
            sql = "SELECT CODE FROM `referral_mapping` where ACTIVE=1 AND CODE=%s"
            cursor.execute(sql, code)
            result = cursor.fetchone()
            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='Invalid referral code'))
    else:
        return json(create_response(message='referral code not provided'))

@app.route("/add/referral", methods=['POST'])
async def addReferral(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    dbWrite = pymysql.connect(host=db_write_config["DB_HOST"], user=db_write_config["DB_USER"], password=db_write_config["DB_PASS"], db=db_write_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

    code = request.json.get('code', '')
    uuid = request.json.get('uuid', '')
    name = request.json.get('name', '')
    mobileVerified = request.json.get('mobileVerified', 0)

    if code:
        with dbRead.cursor() as readCursor:
            sql = "SELECT CODE, UUID FROM `referral_mapping` where ACTIVE=1 AND CODE=%s"
            readCursor.execute(sql, code)
            result = readCursor.fetchone()
            if result:
                referrerUuid = result.get('UUID', '')
                if uuid:
                    if referrerUuid == uuid:
                        return json(create_response(message='You can\'t refer yourself'))
                    else:
                        sql = "SELECT ID, UUID, CODE, REFERRAR_UUID, MOBILE_VERIFIED FROM `referral_mapping` where ACTIVE=1 AND UUID='{}'".format(uuid)
                        readCursor.execute(sql)
                        oldData = readCursor.fetchone()
                        print(sql)
                        if oldData:
                            existingReferrarUuid = oldData.get('REFERRAR_UUID', '')
                            if existingReferrarUuid:
                                return json(create_response(success=True, message='This User has already applied a referral code'))
                            else:
                                #Add joining bonus to user account based on the status of his mobile. If verified than provide instant credit otherwise provide unrealised credits
                                if mobileVerified:
                                    userTrans = add_credit(uuid, pointsType='Instant Referral Discount', sourceId=oldData.get('ID', 0), creditType='REALISED')
                                else:
                                    userTrans = add_credit(uuid, pointsType='Instant Referral Discount', sourceId=oldData.get('ID', 0), creditType='UNREALISED')

                                ##############  Handling of the senario where the referrer has already recived the maximum amount of bonus he should be allowed to recive ############## 
                                refTrans = {}
                                if should_user_get_referral_bonus(referrerUuid):
                                    refTrans = add_credit(referrerUuid, pointsType='Referral Points', sourceId=oldData.get('ID', 0), creditType='UNREALISED')
        
                                print('+++++++++++++++++++++++++++++++')
                                print('++++++++Transaction status+++++')
                                print(userTrans)
                                print(refTrans)
                                print('===============================')

                                if userTrans.get('status', False):
                                    with dbWrite.cursor() as writeCursor:
                                        if refTrans.get('status', False):
                                            sql = "UPDATE `referral_mapping` SET REFERRAR_UUID='{1}', MOBILE_VERIFIED={2}, JOINING_BONUS=1, REFERRAL_BONUS=1 where ACTIVE=1 AND UUID='{0}'".format(uuid, referrerUuid, mobileVerified)
                                        else :
                                            sql = "UPDATE `referral_mapping` SET REFERRAR_UUID='{1}', MOBILE_VERIFIED={2}, JOINING_BONUS=1, REFERRAL_BONUS=0 where ACTIVE=1 AND UUID='{0}'".format(uuid, referrerUuid, mobileVerified)
                                        print(sql)
                                        writeCursor.execute(sql)
                                        result = {
                                            "mobileVerified": mobileVerified,
                                            "instant_discount_received": userTrans.get('status', False),
                                            "bonus_received": refTrans.get('status', False),
                                            "code": oldData.get('CODE', '')
                                        }
                                        return json(create_response(True, result))
                                else:
                                    return json(create_response(message=userTrans.get('message', '')+'. '+refTrans.get('message', '')))

                        else:
                            userCode = code_generator(default=name)
                            with dbWrite.cursor() as writeCursor:
                                sql = "INSERT INTO `referral_mapping` (UUID, CODE, REFERRAR_UUID, MOBILE_VERIFIED, JOINING_BONUS, REFERRAL_BONUS) VALUES('{0}', '{1}', '{2}', {3}, 1, 1)".format(uuid, userCode, referrerUuid, mobileVerified)
                                print(sql)
                                writeCursor.execute(sql)
                                
                                insertId = writeCursor.lastrowid

                                if mobileVerified:
                                    userTrans = add_credit(uuid, pointsType='Instant Referral Discount', sourceId=insertId, creditType='REALISED')
                                else:
                                    userTrans = add_credit(uuid, pointsType='Instant Referral Discount', sourceId=insertId, creditType='UNREALISED')

                                ##############  Handling of the senario where the referrer has already recived the maximum amount of bonus he should be allowed to recive ############## 
                                refTrans = {}
                                if should_user_get_referral_bonus(referrerUuid) & userTrans.get('status', False):
                                    refTrans = add_credit(referrerUuid, pointsType='Referral Points', sourceId=insertId, creditType='UNREALISED')

                                if userTrans.get('status', False):
                                    if not refTrans.get('status', False):
                                        with dbWrite.cursor() as writeCursor:
                                            sql = "UPDATE `referral_mapping` SET REFERRAL_BONUS=0 where ID={0}".format(insertId)
                                            print(sql)
                                            writeCursor.execute(sql)
                                    result = {
                                        "mobileVerified": mobileVerified,
                                        "instant_discount_received": userTrans.get('status', False),
                                        "bonus_received": refTrans.get('status', False),
                                        "code": userCode
                                    }
                                    return json(create_response(True, result))
                                else:
                                    with dbWrite.cursor() as writeCursor:
                                        sql = "DELETE FROM `referral_mapping` where ID={0}".format(insertId)
                                        print(sql)
                                        writeCursor.execute(sql)
                                    return json(create_response(message=userTrans.get('message', '')+'. '+refTrans.get('message', '')))
                else:
                    return json(create_response(message='UUID not provided'))
            else:
                return json(create_response(message='referral code invalid'))
    else:
        return json(create_response(message='referral code not provided'))

@app.route("/revoke/referral", methods=['DELETE'])
async def revokeReferral(request):
    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    dbWrite = pymysql.connect(host=db_write_config["DB_HOST"], user=db_write_config["DB_USER"], password=db_write_config["DB_PASS"], db=db_write_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    
    uuid = request.json.get('uuid', '')
    
    if uuid:
        with dbRead.cursor() as readCursor:
            sql = "SELECT TRANSACTION_ID, REFERRAL_MAPPING_ID FROM `transactions` WHERE ACTIVE=1 AND AFFECTED_USER_UUID=%s"
            readCursor.execute(sql, uuid)
            result = readCursor.fetchall()
            print(result)
            if result:
                referralMappingIdList = []
                transactionIdList = []
                for transactions in result:
                    transactionId = transactions.get('TRANSACTION_ID', '')
                    inValidateTransactionResponse = revoke_credit(transactionId)
                    
                    if inValidateTransactionResponse.get('status', False):
                        transactionIdList.append(transactionId)
                        referralMappingIdList.append(transactions.get('REFERRAL_MAPPING_ID', ''))
                
                with dbWrite.cursor() as writeCursor:
                    sql = "UPDATE `referral_mapping` SET ACTIVE=0 WHERE ACTIVE=1 AND ID in ('{0}')".format("', '".join(map(str, referralMappingIdList)))
                    writeCursor.execute(sql)
            else:
                return json(create_response(message='Can not find any transaction related to the UUID that you have provided'))
        ret = {
            "updatedIdList": referralMappingIdList,
            "updatedTransactionIdList": transactionIdList
        }
        return json(create_response(True, ret))

    else:
        return json(create_response(message='UUID not provided'))

@app.route("/upload/user_contact", methods=['POST'])
async def uploadUserContact(request):
    uuid = request.json.get('uuid', '')
    deviceId = request.json.get('deviceId', '')
    data = request.json.get('data', '')

    dbRead = pymysql.connect(host=db_read_config["DB_HOST"], user=db_read_config["DB_USER"], password=db_read_config["DB_PASS"], db=db_read_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
    dbWrite = pymysql.connect(host=db_write_config["DB_HOST"], user=db_write_config["DB_USER"], password=db_write_config["DB_PASS"], db=db_write_config["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

    if uuid:
        if deviceId:
            with dbWrite.cursor() as writeCursor:
                for contact in data:
                    id = contact.get('id',0)
                    name = contact.get('name',0)
                    emails = pyjson.dumps(contact.get('emails',0))
                    phoneNumbers = pyjson.dumps(contact.get('phoneNumbers',0))

                    sql = "INSERT IGNORE INTO `user_contacts` (user_id, unique_device_id, contact_id, contact_name, contact_phones, contact_emails) VALUES('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')".format(uuid, deviceId, id, name, phoneNumbers, emails)
                    writeCursor.execute(sql)
            dbWrite.commit()

            producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)

            code = ''

            with dbRead.cursor() as readCursor:
                sql = "SELECT CODE FROM `referral_mapping` where ACTIVE=1 AND UUID='{0}'".format(uuid)
                print(sql)
                readCursor.execute(sql)
                result = readCursor.fetchone()
                code = result.get('CODE', '')

            # print(POINTS_PLAN_OBJECT)

            for contact in data:
                phoneNumbers = contact.get('phoneNumbers',[])
                if phoneNumbers:
                    for number in phoneNumbers:
                        print(number)
                        number = re.sub('[^0-9]','', number)
                        print(number)
                        if number.find('+91') == 0:
                            number = number[3:]
                        print(number)

                        if number.find('0') == 0:
                            number = number[1:]
                        print(number)

                        if len(number) != 10:
                            continue
                        
                        m = {
                            'mobile': number,
                            'countryCode': '+91',
                            'text': "Hi! Your friend has invited you to FabHotels to get Rs. {0} off on your bookings at FabHotels. Register using their referral code {1} or via this link fabhotels.com/invite/{1}".format(POINTS_PLAN_OBJECT['Instant Referral Discount']['points'], code),
                            # 'text': '<variable> off on your future bookings <variable> Register using <variable>',
                            'smsType': 'TEXT'
                        }
                        print(m)
                        future = producer.send(KAFKA_TOPIC, pyjson.dumps(m).encode('utf-8'))

                        producer.flush()

            ret = {}
            return json(create_response(True, ret))
        else:
            return json(create_response(message='deviceId not provided'))
    else:
        return json(create_response(message='UUID not provided'))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, workers=10)