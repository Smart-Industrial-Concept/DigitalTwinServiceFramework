#%%
from kafka import KafkaConsumer
from kafka import KafkaProducer

from time import sleep


from rdflib import Graph, ConjunctiveGraph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD
from rdflib.plugins.stores import sparqlstore
from rdflib.plugins.sparql.processor import SPARQLResult

import json
from json import dumps
from json import loads

import pandas as pd

from SPARQLWrapper import SPARQLWrapper, JSON, JSONLD, XML, RDFXML, TURTLE
import requests

from collections import defaultdict


import logging

#setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
formatter=logging.Formatter(LOG_FORMAT)
stream_handler=logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

#setup connection info for Kafka and SPARQL endpoint
requestTopic="evaluationServiceRequest"

#here adapt!!!
#kafkaServerAddress='localhost:9092'  # change to kafka:9093 if containerized
#causalRelationEndpoint="http://localhost:8086/rdf4j-server/repositories/causalrelations"
#update_endpoint = "http://localhost:8086/rdf4j-server/repositories/evalresutls/statements"

kafkaServerAddress='kafka:9093'
causalRelationEndpoint="http://rdf4j-eval:8080/rdf4j-server/repositories/causalrelations"
update_endpoint = "http://rdf4j-eval:8080/rdf4j-server/repositories/evalresutls/statements"


logger.info("Setting up connection for Kafka @ %s ", kafkaServerAddress)

#if kafka is not ready, it will be retried to create producer and consumer
while(True):
    try:
        consumer = KafkaConsumer(
            requestTopic,
            bootstrap_servers=[kafkaServerAddress], # change to 9093 if containerized
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            group_id='my-group-id',
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

def sparql_results_to_df(results: SPARQLResult):
    """
    Export results from an rdflib SPARQL query into a `pandas.DataFrame`,
    using Python types. See https://github.com/RDFLib/rdflib/issues/1179.
    """
    return pd.DataFrame(
        data=([None if x is None else x.toPython() for x in row] for row in results),
        columns=[str(x) for x in results.vars],
    )

def transformAnomaliesToDataframe(g):
    PETI=Namespace("http://auto.tuwien.ac.at/sic/PETIont#")
    OWLTIME=Namespace("http://www.w3.org/2006/time#")
    ns=dict(peti=PETI, time=OWLTIME)

    result=g.query(""" 
    SELECT ?anomaly ?sensor  ?startTime ?endTime
    WHERE {?anomaly a peti:Anomaly;
                    ^peti:hasAnomaly ?sensor;
                    time:hasBeginning/time:inXSDDateTimeStamp ?startTime;
                    time:hasEnd/time:inXSDDateTimeStamp ?endTime.
    }""", initNs=ns)
    df=sparql_results_to_df(result)

    return df

def groupAnomaliesToIncidences(df):
    incidencesIndexes=list()
    indexList=list(range(len(df)))
   
    while indexList:
        i=indexList.pop(0)
        currentTimes=df[['startTime','endTime']].iloc[i]
        currentList=[i]

        tempList=[]
        for j in indexList:
            iterTimes=df[['startTime','endTime']].iloc[j]

            if (iterTimes <= currentTimes+pd.Timedelta(minutes=4)).all() and (iterTimes >= currentTimes-pd.Timedelta(minutes=4)).all():
                currentList.append(j)
                tempList.append(j)   

        #remove temp from list
        indexList= [x for x in indexList if x not in tempList]
        incidencesIndexes.append(currentList)

    #tranform indexes into sensors
    incidences=list()
    for i in range(len(incidencesIndexes)):
        incidences.append(df['sensor'].iloc[incidencesIndexes[i]].values.tolist())
    
    return incidences, incidencesIndexes

def findRootNodeOfIncident(incident, causalRelationEndpoint):    
    #genreate sparql query
    inputString=""
    for i in range(len(incident)):
        inputString=inputString+"<"+incident[i]+">\n"

    queryString="""
    PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>

    SELECT ?sensor ?prevSensor
    WHERE {
    VALUES ?sensor{""" + inputString + """ 
        }  
    ?sensor ^peti:hasMeasurement ?virtualProperty.
    ?virtualProperty ^peti:hasInfluenceOn/^peti:hasInfluenceOn/peti:hasEquivalentProperty* ?otherProperty.
    ?otherProperty peti:hasMeasurement ?prevSensor.
    
    VALUES ?prevSensor{"""+ inputString + """
    }
    }
    """
    sparql = SPARQLWrapper(causalRelationEndpoint)
    sparql.setQuery(queryString)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()


    resultGraph = defaultdict(list)

    #create graph based on pytho dict
    for result in results["results"]["bindings"]:
        #print('%s %s' % (result["sensor"]["value"], result["prevSensor"]["value"]))
        resultGraph[result["prevSensor"]["value"]]
        resultGraph[result["sensor"]["value"]].append(result["prevSensor"]["value"])

    #get root node
    rootNode=None

    for key in resultGraph:
        if len(resultGraph[key]) == 0:
            rootNode= key
            break
    
    return rootNode

def findImplicitRedundantSensors(rootNode):
    queryString="""
    PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>
    SELECT ?influencedSensor
    WHERE {
        
        <""" + rootNode + """> ^peti:hasMeasurement ?virtualProperty.
        ?virtualProperty peti:hasInfluenceOn/peti:hasInfluenceOn ?otherVirtualProperty.
        ?otherVirtualProperty peti:hasMeasurement ?influencedSensor.
    }"""
    sparql = SPARQLWrapper(causalRelationEndpoint)
    sparql.setQuery(queryString)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    influencedSensorList=list()
    for result in results["results"]["bindings"]:
        influencedSensorList.append([result["influencedSensor"]["value"]])
    
    return influencedSensorList

def identifyRootCauseAndStoreResult(incident, incidentID, influencedSensorList, rootNode, anomalyList,update_endpoint):
                
    insertString=""
    #create peti:incidence instance
    insertString = insertString + "peti:incident_0 a peti:Incident.\n" 
   
    if len(influencedSensorList)> 0 and  len(influencedSensorList) >= (len(incident)-1)/2:
        logger.info("Sensor Fault")
        rootCause="SensorFault"
        insertString = insertString + "peti:incident_"+str(incidentID)+" a peti:SensorFault.\n"
        #add rootsensor
        insertString = insertString + "peti:incident_"+str(incidentID)+" peti:hasRootCause <"+ rootNode +">.\n"
    else:
        logger.info("Anormal Behavior")
        insertString = insertString + "peti:incident_"+str(incidentID)+" a peti:AbnormalBehavior.\n"
        rootCause="AbnormalBehavior"
    
    #link incident with anomalies
    for element in anomalyList:
        insertString=insertString + "peti:incident_"+str(incidentID)+" peti:hasRelatedAnomaly <" + element +">.\n"

    

    sparqlString="""
    PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    INSERT { """ + insertString + """
    } WHERE {}
    """

    #query_endpoint = 'http://localhost:8086/rdf4j-server/repositories/evalresutls/'
    requests.post(update_endpoint, data={'content-type':'application/sparql-update','update': sparqlString, })#application/ld+json

    return rootCause, insertString

def main():
    #wait for incomming event
    logger.info("Waiting for requests...")
    for event in consumer:
    
        event_data = event.value
        consumer.commit()
        #get reply topic and message id 
        if 'variablesAsMap' in event_data:
            replyTopic=event_data['variablesAsMap']['replyTopic']
            replyID=event_data['variablesAsMap']['id']
            anomalies=event_data['variablesAsMap']['anomalies']
        else:
            replyTopic=event_data['replyTopic']
            replyID=event_data['id']
            anomalies=event_data['anomalies']
        logger.info("Message received with ID: " + replyID)

       
        #convert anomalies to rdf
        g = Graph().parse(data=anomalies, format='json-ld')
        
        df=transformAnomaliesToDataframe(g)
        
        incidences, incidencesIndexes=groupAnomaliesToIncidences(df)
        
        #check for every incidence if there are more sensors involved. if yes, find root node
        evaluationList=list()
        index=0
        for incident in incidences:
            
            #found incident with more than one anomalies
            if len(incident) > 1:
                rootNode=findRootNodeOfIncident(incident,causalRelationEndpoint)
                influencedSensorList=findImplicitRedundantSensors(rootNode)
            else:
                rootNode=incident[0]
                influencedSensorList=[]
            #get List of anomalies for this incident
            anomalyList=df['anomaly'].iloc[incidencesIndexes[index]].values.tolist()
            rootCause,result=identifyRootCauseAndStoreResult(incident, index, influencedSensorList, rootNode, anomalyList,update_endpoint)

            evaluationList.append((rootCause,result))
            index=index+1

        faultType="AbnormalBehavior"
        for el in evaluationList:
            if el[0]=="SensorFault":
                faultType="SensorFault"
                break

        #create answer in json-LD  
        prefixString="@prefix peti: <http://auto.tuwien.ac.at/sic/PETIont#>.\n"
        g=Graph()
        for el in evaluationList:
            parseString=prefixString+el[1]
            g.parse(data=parseString, format='n3')
        
        answer= g.serialize(format='json-ld', indent=4).decode("utf-8")
        
        #reply message
        header={'id':replyID,
        'replyTopic':replyTopic,
        'type':faultType
        }
        
        payload = {'evalResult': answer}
        message=header | payload

        logger.info("Sending reply message to: "+ replyTopic +" with ID: "+ replyID)
        producer.send(replyTopic, value=message)
        producer.flush() 
    

    logger.info("Smart Service is terminated!")


if __name__ == "__main__":
    main()