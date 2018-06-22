from sanic import Sanic
from sanic.response import json
import string
from datetime import datetime
import random
import pymysql
import pymysql.cursors
import requests

# POINTS_SERVICE_URL = 'http://13.127.243.15:8080'

POINTS_SERVICE_URL = 'http://172.31.7.216:8080'

app = Sanic()

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

TRANSACTION_TYPE = {
    "UNREALISEDCREDIT": 0,
    "CREDIT": 1
}

CREDIT_TYPE = {
    "Referral Points": 1,
    "Instant Referral Discount": 0

}

dbRead = pymysql.connect(host=db_settings["DB_HOST"], user=db_settings["DB_USER"], password=db_settings["DB_PASS"], db=db_settings["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)
dbWrite = pymysql.connect(host=db_settings["DB_HOST"], user=db_settings["DB_USER"], password=db_settings["DB_PASS"], db=db_settings["DB_NAME"], charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor, autocommit=True)

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

def commit_transaction(responseData, referralMappingId, uuid, creditType):
    print(responseData)
    transactionId = responseData.get('transactionId', '')
    transactionType = TRANSACTION_TYPE.get(responseData.get('transactionType'))
    
    creditType = CREDIT_TYPE.get(creditType)
    with dbWrite.cursor() as writeCursor:
        sql = "INSERT INTO transactions (TRANSACTION_ID, TRANSACTION_TYPE, REFERRAL_MAPPING_ID, AFFECTED_USER_UUID, DISCOUNT_TYPE) VALUES ('{0}', {1}, {2}, '{3}', {4})".format(transactionId, transactionType, referralMappingId, uuid, creditType)
        print(sql)
        writeCursor.execute(sql)
        result = writeCursor.fetchone()

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
    commit_transaction(jsonResponse.get('data'), sourceId, uuid, pointsType)
    return jsonResponse


# function to generate referral code
def code_generator(size=6, default='', chars=string.ascii_uppercase.replace("O","") +string.digits.replace("0","")):
    while True:
        if default:
            unique_code = default[0:10]
            default = ''
        else:
            unique_code = ''.join(random.choice(chars) for _ in range(size))
        with dbRead.cursor() as cursor:
            sql = "SELECT CODE FROM `referral_mapping` WHERE `CODE`=%s"
            cursor.execute(sql, unique_code)
            result = cursor.fetchone()
            if not result:
                break
    return unique_code

def create_response(success=False, data={}, message=''):
    ret = {
        "success": success,
        "data": data,
        "message": message
    }
    return ret

@app.route("/ref/code", methods=['POST'])
async def getOrGeneratCode(request):
    name = request.json.get('name', '')
    uuid = request.json.get('uuid', '')
    mobileVerified = request.json.get('mobileVerified', 1)
    print(request.json)

    if uuid:
        with dbRead.cursor() as cursor:
            sql = "SELECT CODE, MOBILE_VERIFIED, JOINING_BONUS, REFERRAL_BONUS FROM `referral_mapping` WHERE `UUID`=%s"
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
    uuid = request.json.get('uuid', '')

    if uuid:
        with dbRead.cursor() as cursor:
            sql = "SELECT count(*) as referred_count FROM `referral_mapping` WHERE `REFERRAR_UUID`=%s"
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

    code = request.json.get('code', '')

    if code:
        with dbRead.cursor() as cursor:
            sql = "SELECT UUID FROM `referral_mapping` where CODE=%s"
            cursor.execute(sql, code)
            result = cursor.fetchone()
            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='referral code not found'))
    else:
        return json(create_response(message='referral code not provided'))

@app.route("/get/event_detail", methods=['POST'])
async def getUserDetailsFromMappingId(request):

    idList = request.json.get('idList', '')

    if idList:
        with dbRead.cursor() as cursor:
            sql = "SELECT ID, UUID, REFERRAR_UUID, MOBILE_VERIFIED, FIRST_CHECKOUT, CODE FROM `referral_mapping` where ID in ({0})".format(', '.join(str(ids) for ids in idList))
            cursor.execute(sql)
            result = cursor.fetchall()
            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='details not found for provided IDs'))
    else:
        return json(create_response(message='Id list not provided'))

@app.route("/check/code", methods=['POST'])
async def isValidReferralCode(request):

    code = request.json.get('code', '')

    if code:
        with dbRead.cursor() as cursor:
            sql = "SELECT CODE FROM `referral_mapping` where CODE=%s"
            cursor.execute(sql, code)
            result = cursor.fetchone()
            if result:
                return json(create_response(True, result))
            else:
                return json(create_response(message='referral code invalid'))
    else:
        return json(create_response(message='referral code not provided'))

@app.route("/add/referral", methods=['POST'])
async def addReferral(request):

    code = request.json.get('code', '')
    uuid = request.json.get('uuid', '')
    name = request.json.get('name', '')
    mobileVerified = request.json.get('mobileVerified', 0)

    if code:
        with dbRead.cursor() as readCursor:
            sql = "SELECT CODE, UUID FROM `referral_mapping` where CODE=%s"
            readCursor.execute(sql, code)
            result = readCursor.fetchone()
            if result:
                referrerUuid = result.get('UUID', '')
                if uuid:
                    if referrerUuid == uuid:
                        return json(create_response(message='You can\'t refer yourself'))
                    else:
                        sql = "SELECT ID, UUID, CODE, REFERRAR_UUID, MOBILE_VERIFIED FROM `referral_mapping` where UUID='{}'".format(uuid)
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
                                refTrans = add_credit(referrerUuid, pointsType='Referral Points', sourceId=oldData.get('ID', 0), creditType='UNREALISED')
        
                                if userTrans.get('status', False) & refTrans.get('status', False):
                                    with dbWrite.cursor() as writeCursor:
                                        sql = "UPDATE `referral_mapping` SET REFERRAR_UUID='{1}', MOBILE_VERIFIED={2}, JOINING_BONUS=1, REFERRAL_BONUS=1 where UUID='{0}'".format(uuid, referrerUuid, mobileVerified)
                                        print(sql)
                                        writeCursor.execute(sql)
                                        result = {
                                            "mobileVerified": mobileVerified,
                                            "instant_discount_received": True,
                                            "bonus_received": True,
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
                                refTrans = add_credit(referrerUuid, pointsType='Referral Points', sourceId=insertId, creditType='UNREALISED')
                                if userTrans.get('status', False) & refTrans.get('status', False):
                                    result = {
                                        "mobileVerified": mobileVerified,
                                        "instant_discount_received": True,
                                        "bonus_received": True,
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, workers=4)