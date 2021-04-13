#%%
from kafka import KafkaProducer
from kafka import KafkaConsumer
from json import loads
from json import dumps

import time
import datetime
import pprint

from kafka_request_reply import RequestReplyHandler

import pandas as pd
#%%
requestTopic="anomalyServiceRequest"
replyTopic="replyToTestAnomalyService"

kafkaServerAddress='localhost:9092'  

payload = {'action': 'getAnomalies'}  

rrh=RequestReplyHandler.getInstance(requestTopic,replyTopic,kafkaServerAddress)


res=rrh.syncRequest(payload)
print(res)