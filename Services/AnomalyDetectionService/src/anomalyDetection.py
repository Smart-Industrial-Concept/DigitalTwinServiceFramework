#%%
from kafka import KafkaConsumer
from kafka import KafkaProducer

import pandas as pd
#import matplotlib.pyplot as plt
import numpy as np

import myUtils as ut
import pyprog

import pickle
from kafka_request_reply import RequestReplyHandler

from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD

from rdflib.plugins.stores import sparqlstore
from json import dumps
from json import loads
import logging
from time import sleep

import traceback
import sys

#setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
formatter=logging.Formatter(LOG_FORMAT)

stream_handler=logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

#Fuseki
update_endpoint = 'http://fuseki_anomaly:3030/anomalies/update'  # localhost:3031// change to fuseki_anomaly:3030 if containerized
query_endpoint = 'http://fuseki_anomaly:3030/anomalies/query'    # change to fuseki_anomaly:3030 if containerized
#update_endpoint = 'http://localhost:3031/anomalies/update'  # localhost:3031// change to fuseki_anomaly:3030 if containerized
#query_endpoint = 'http://localhost:3031/anomalies/query'

#kafka
requestTopic="anomalyServiceRequest"

smarDataRQTopic="smartDataRequest"
smartDataRPTopic="smartDataReplyToAnomalyService"
kafkaServerAddress='kafka:9093'  # change to kafka:9093 if containerized /run outside localhost:9092
#kafkaServerAddress='localhost:9092'

logger.info("Setting up connection for Kafka @ %s", kafkaServerAddress)
while(True):
    try:
        consumer = KafkaConsumer(
            requestTopic,
            bootstrap_servers=[kafkaServerAddress], # change to 9093 if containerized
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            group_id='anomalyGroup',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

        producer = KafkaProducer(
            bootstrap_servers=[kafkaServerAddress],
            value_serializer=lambda x: dumps(x).encode('utf-8'), #convert to json string and encode as utf-8
            max_request_size=15728640
        )
        break
    except:
        print("Cannot connect to Kafka... retry in 5 Seconds...")
        sleep(5)

#%% import already trained models and statistics about residuals
def loadModels():
    file =open('../misc/trainedModels.pickle','rb')
    trainedModels=pickle.load(file)
    file.close()

    residualInfo=pd.read_csv('../misc/residualInfo.csv')
    residualInfo.set_index(['Unnamed: 0'],inplace=True)
    residualInfo.index.names=['pce']

    return trainedModels, residualInfo

def getDataForPCE(rrh,pceList):
    '''sends a sparql query to retrive sensor data for a List of PCEs '''

    pceStringList=""
    for pce in pceList:
        pceStringList=pceStringList+"peti:"+pce+" "

    queryString= """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>
            PREFIX sosa: <http://www.w3.org/ns/sosa/>

            SELECT ?pce ?time ?value 
            WHERE {
                VALUES ?pce { """+ pceStringList +"""}

                {   ?pce  sosa:madeObservation ?obs.}
                union
                {   ?pce sosa:madeActuation ?obs.}
                
                ?obs sosa:resultTime ?time.
                ?obs sosa:hasSimpleResult ?value.
            }
            """

    payload = {'query': queryString} 
    res=rrh.syncRequest(payload)
    
    if res == "ERROR": #ToDo use exception here
        print("Error was returend by Smart Service")
        raise Exception("Smart Data Service unavailable")
    
    h=res['results']

    #create dataframe from response
    df=pd.json_normalize(h['bindings'])

    #bring table into right shape
    evalData=df[['pce.value','time.value', 'value.value']]
    evalData.columns=['pce','time','value']
    evalData=evalData.pivot(index='time', columns='pce', values="value")
    evalData.index=pd.to_datetime(evalData.index)

    return evalData


#%% request senor data from "Smart Data Service" 
# init kafka
def requestSensorData(trainedModels,smarDataRQTopic,smartDataRPTopic,kafkaServerAddress):
    rrh=RequestReplyHandler.getInstance(smarDataRQTopic,smartDataRPTopic,kafkaServerAddress)

    ##get data from Smart Service
    pceList=[]
    for key in trainedModels:
        pceList=pceList+(trainedModels[key]['features']['input'])
        pceList=pceList+(trainedModels[key]['features']['output'])
    #remove duplicates
    pceList=list(set(pceList))
    
    data=getDataForPCE(rrh,pceList)
    data=data.apply(pd.to_numeric)
    #Todo- check return value for error AND Dimension!

    return data


#%%   find anomaly by simulating use case
def findAnomalies(data,trainedModels,residualInfo):
    #%start sim  
    windowSize=20
    workingDataForPrediction=data.copy()
    workingDataForPrediction.reset_index(inplace=True, drop=True)
    workingDataForPrediction.columns=['Actuator_Fan1','Actuator_Fan2','Actuator_H1','T_ext','T_hx','T_in', 'T_p','T_sup','m_in','m_out']

    #not used here: -
    predResults, predResiduals=ut.simulateUseCaseSerialParallelModel(trainedModels,workingDataForPrediction,1,len(workingDataForPrediction)-1,windowSize)
    #results=pd.DataFrame(predResults,columns=['m_in','T_sup','T_hx','T_p'])
    #measuredData=workingDataForPrediction[['m_in','T_sup','T_hx','T_p']]

    #residual table
    predResidualTable=pd.DataFrame(predResiduals,columns=['m_in','T_sup','T_hx','T_p'])

    #define threshholds
    threshholds_high= residualInfo['mean'] + residualInfo['std']*3
    threshholds_low= residualInfo['mean'] - residualInfo['std']*3

    #find values which exceed threshhold (true/fals)
    t=predResidualTable[predResidualTable > threshholds_high]
    t2=predResidualTable[ predResidualTable < threshholds_low]
    t.update(t2)
    t.dropna(axis=0, how='all')

    #get index for beginning and end of the fault
    faultIndexList_m=ut.returnFaultIndexs(t['m_in'])
    faultIndexList_T_sup=ut.returnFaultIndexs(t['T_sup'])
    faultIndexList_T_hx=ut.returnFaultIndexs(t['T_hx'])
    faultIndexList_T_p=ut.returnFaultIndexs(t['T_p'])

    faultIndexes={'m_in':faultIndexList_m, 'T_sup':faultIndexList_T_sup, 'T_hx':faultIndexList_T_hx, 'T_p':faultIndexList_T_p}

    #convertindex to timestamp and store in "anomaliesDict"
    anomaliesDict={}
    for key in faultIndexes:
        anomaliesDict[key]=ut.getTimeStampFromIndex(faultIndexes[key], data.index)

    return anomaliesDict

#%%main loop

def main():
    logger.info("Anomaly Service has Started")
    trainedModels, residualInfo = loadModels()

    #wait for incomming event
    logger.info("Waiting for requests...")
    for event in consumer:
        
        event_data = event.value 
        consumer.commit()
        logger.debug("Raw event value received: %s", event_data)
        #get reply topic and message id, but check how message is formated
        if 'variablesAsMap' in event_data:
            replyTopic=event_data['variablesAsMap']['replyTopic']
            replyID=event_data['variablesAsMap']['id']
        else:
            replyTopic=event_data['replyTopic']
            replyID=event_data['id']
        
        logger.info("Message received with ID: " + replyID +" to reply topic: " +replyTopic)

        #request data/ find anomalies / return answe
        logger.info("Reqeust data from Smart Data Service")

        try:
            data=requestSensorData(trainedModels,smarDataRQTopic,smartDataRPTopic,kafkaServerAddress)
        
             
            logger.info("Find anomalies...")
            anomaliesDict=findAnomalies(data,trainedModels,residualInfo)
            logger.info("Write graph to SPARQL Endpoint")
            g=ut.storeAnomalyAtSparqlEndpoint(anomaliesDict,query_endpoint,update_endpoint)
            answer= g.serialize(format='json-ld', indent=4).decode("utf-8")
        except:
            traceback.print_exception(*sys.exc_info())
            
            answer="ERROR"
        #reply message
        header={'id':replyID,
        'replyTopic':replyTopic
        }
        
        payload = {'anomalies': answer}
        message=header | payload

        logger.info("Sending reply message to "+ replyTopic +" with ID: "+ replyID)
        producer.send(replyTopic, value=message)
        producer.flush()        

    logger.info("Anomaly Detection Service has terminated!")
if __name__ == "__main__":
    main()