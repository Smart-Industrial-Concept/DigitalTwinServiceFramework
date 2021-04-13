from kafka import KafkaConsumer
from kafka import KafkaProducer
import logging
import time
import datetime
from json import loads
from json import dumps

#confluent
# from confluent_kafka import Consumer
# from confluent_kafka import Producer

#setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
formatter=logging.Formatter(LOG_FORMAT)

stream_handler=logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class RequestReplyHandler:
    """ realized as Singelton pattern"""
    __instance=None

    @staticmethod
    def getInstance(requestTopic,replyTopic, kafkaServerAddress):
        """static access method"""
        if RequestReplyHandler.__instance == None:
            print("new Instance")
            RequestReplyHandler(requestTopic,replyTopic, kafkaServerAddress)
        return RequestReplyHandler.__instance

    def __init__(self, requestTopic,replyTopic, kafkaServerAddress):

        if RequestReplyHandler.__instance != None:
            raise Exception ("RequestReplyHandler is a singelton")
        else:
            print("create")
            self.requestTopic=requestTopic
            self.replyTopic=replyTopic
            self.kafkaServerAddress=kafkaServerAddress

            self.producer = KafkaProducer(
                bootstrap_servers=[kafkaServerAddress],
                value_serializer=lambda x: dumps(x).encode('utf-8'),#convert to json string and encode as utf-8
                max_request_size=15728640) 
            
            #confluent
            # conf = {'bootstrap.servers': kafkaServerAddress,'client.id': socket.gethostname(), 'max.request.size':15728640}
            # self.producer = Producer(conf)

            
            self.consumer = KafkaConsumer(
                replyTopic,
                bootstrap_servers=[kafkaServerAddress],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                group_id='my-group-id',
                value_deserializer=lambda x: loads(x.decode('utf-8')))
            
            #confluent
            # conf = {'bootstrap.servers': kafkaServerAddress,
            #         'group.id': 'my-group-id',
            #         'auto.offset.reset': 'latest'}
            # self.consumer = Consumer(replyTopic)
            # self.consumer.subscribe(topics)

            RequestReplyHandler.__instance = self
    
    #def __del__(self):
        #self.producer.close()
        #self.consumer.close()

    def createMessageID(self):
        """just a simple time-based id creation"""
        return hex(int(time.time()))

    def syncRequest(self,requestMessagePyload):
        """performs a synchronous service call on a specified topic, waiting for the response over a reply topic.
        A messeage id is used to correlate the request with the reply"""
        id=self.createMessageID() 
        
        #create messafe with ID, reply topic and pyload
        header={'id':id,
                'replyTopic':self.replyTopic
        }
        payload = requestMessagePyload  
        message=header | payload

        logger.debug("Sending message with ID: %s to topic: %s",id, self.requestTopic)
        self.producer.send(self.requestTopic, value=message)
        self.producer.flush()
        
        #confluent
        # self.producer.produce(self.requestTopic, value=message)
        # self.producer.flush()

        logger.debug("Waiting for response...")
        for event in self.consumer:
            
            event_data = event.value  
            replyID=event_data['id']
            data=event_data['reply']

            logger.debug("...response received with ID: " + replyID)  
            self.consumer.commit()

            if replyID == id:
                replyMessage=data
                break

        return replyMessage
        # while True:
        # # Response format is {TopicPartiton('topic1', 1): [msg1, msg2]}
        #     msg_pack = self.consumer.poll(timeout_ms=100)

        #     for tp, messages in msg_pack.items():
        #         for message in messages:
        #             # message value and key are raw bytes -- decode if necessary!
        #             # e.g., for unicode: `message.value.decode('utf-8')`
        #             print ("%s:%d:%d: key=%s value=%s" % (tp.topic, tp.partition,
        #                                                 message.offset, message.key,
        #                                                 message.value))

        #         return "tesreturn"
        
# timeout is missing