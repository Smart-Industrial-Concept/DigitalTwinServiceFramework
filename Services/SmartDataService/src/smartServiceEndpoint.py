from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
from time import sleep
from json import dumps

from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import logging

#setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
formatter=logging.Formatter(LOG_FORMAT)

stream_handler=logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

#logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


#setup connection info for Kafka and SPARQL endpoint
requestTopic="smartDataRequest"

#here adapt!!!
#kafkaServerAddress='localhost:9092'  # change to kafka:9093 if containerized
kafkaServerAddress='kafka:9093'
#federatedSPARQLQueryEndpoint="http://localhost:8080/rdf4j-server/repositories/federatedendpoint" #change to rdf4j:8080 if containerized
federatedSPARQLQueryEndpoint="http://rdf4j:8080/rdf4j-server/repositories/federatedendpoint"

logger.info("Setting up connection for Kafka @ %s and SPARQL enpoint @ %s", kafkaServerAddress, federatedSPARQLQueryEndpoint)

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

def makeSPARQLRequest(queryString):
    """sends the SPARQL request to the endpoing"""
    response = requests.post(federatedSPARQLQueryEndpoint, data={'Accept':'application/sparql-results+json','query': queryString, })#application/ld+json
    data = response.json()

    return data   
  
def main():
    #wait for incomming event
    logger.info("Waiting for requests...")
    for event in consumer:
    
        event_data = event.value
        consumer.commit()
        #get reply topic and message id 
        replyTopic=event_data['replyTopic']
        replyID=event_data['id']
        queryString=event_data['query']

        logger.info("Message received with ID: " + replyID)

        #create answer
        logger.debug("Send SPARQL query to endpoint")
        try:
            answer=makeSPARQLRequest(queryString)
        except:
            logger.error("Connection with SPARQL endpoint failed")
            answer="ERROR"

        #log return message
        logger.debug("SPARQL answer length: " + str(len(str(answer))))
  
        #reply message
        header={'id':replyID,
        'replyTopic':replyTopic
        }
        
        payload = {'reply': answer}
        message=header | payload

        logger.info("Sending reply message to: "+ replyTopic +" with ID: "+ replyID)
        producer.send(replyTopic, value=message)
        producer.flush() 
    
        

    logger.info("Smart Service is terminated!")


if __name__ == "__main__":
    main()
