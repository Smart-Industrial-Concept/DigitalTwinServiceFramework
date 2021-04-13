#%%
from kafka import KafkaConsumer
from kafka import KafkaProducer

from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD

from rdflib.plugins.stores import sparqlstore
from json import dumps
from json import loads
import logging
from time import sleep


#setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
formatter=logging.Formatter(LOG_FORMAT)

stream_handler=logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


#kafka
requestTopic="rootCauseServiceRequest"

#kafkaServerAddress='kafka:9093'  # change to kafka:9093 if containerized /run outside localhost:9092
kafkaServerAddress='localhost:9092'

logger.info("Setting up connection for Kafka @ %s", kafkaServerAddress)
while(True):
    try:
        consumer = KafkaConsumer(
            requestTopic,
            bootstrap_servers=[kafkaServerAddress], # change to 9093 if containerized
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            group_id='rootCauseGroup',
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


logger.info("Root Cause Service has Started!")
#%%
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


    #create answer
    answer = "Hello!"
    #reply message
    header={'id':replyID,
    'replyTopic':replyTopic
    }
    
    payload = {'reply': answer}
    message=header | payload

    logger.info("Sending reply message to "+ replyTopic +" with ID: "+ replyID)
    producer.send(replyTopic, value=message)
    producer.flush()    