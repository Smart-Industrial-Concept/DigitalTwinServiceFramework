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
requestTopic="smartDataRequest"
replyTopic="replyToTestSmartData"

kafkaServerAddress='localhost:9092'  # change to kafka:9093 if containerized

queryString1="""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>
            PREFIX sosa: <http://www.w3.org/ns/sosa/>

            SELECT ?pce ?time ?value 
            WHERE {
                ?pce a peti:PCE-Request;
                     sosa:madeObservation ?obs.
                ?obs sosa:resultTime ?time; 
                     sosa:hasSimpleResult ?value.
            }


          LIMIT 10
            """


payload = {'query': queryString1}  

rrh=RequestReplyHandler.getInstance(requestTopic,replyTopic,kafkaServerAddress)


res=rrh.syncRequest(payload)
print("...reveived reply")

#%%
if res == "ERROR":
    print("Error was returend by Smart Service")
    exit(0)
    
h=res['results']

df=pd.json_normalize(h['bindings'])

#print(h)