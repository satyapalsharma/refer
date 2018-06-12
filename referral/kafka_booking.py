import json
import requests
from threading import Thread
from kafka import KafkaConsumer,logging

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
    consumer = KafkaConsumer('bookingpoints',bootstrap_servers='13.127.227.239:9092', auto_offset_reset='earliest')
    print(consumer)
    while(True):
        print('inside loop')
        if consumer:
            for msg in consumer:
                data = eval(msg.value)
                print ('data',data)
                print ('got data')
    consumer.close()

kafkaCall()

# def run():
#     run_thread = Thread(target=kafkaCall())
#     run_thread.daemon = True
#     run_thread.start()
#     time.sleep(20)