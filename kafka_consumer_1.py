import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import pandas as pd


API_KEY = 'SMHLNBFJB34A3VFY'
ENDPOINT_SCHEMA_URL  = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'SgFlo7iHO6mL/5Dzilt+jveCU1Sd79P+5w5fH3YRokjxVD3rz+Tnu5rHyI95V7rk'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '3HNRJEMH4XNS77IT'
SCHEMA_REGISTRY_API_SECRET = 'vp6ElyzLRpQRyQhbSNdaICL9dRrdC1dfP/Oq0tjHFyt1Mwh9bCSdgpwA98YvkTWA'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Car:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_car(data:dict,ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):
    schema_registry_conf = SchemaRegistryClient(schema_config())
    Topic = topic
    my_schema = schema_registry_conf.get_latest_version(Topic+'-value').schema.schema_str
    

    json_deserializer = JSONDeserializer(my_schema,
                                         from_dict=Car.dict_to_car)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            
            if car is not None:
                print("User record {}: car: {}\n"
                      .format(msg.key(), car))

        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurant-take-away-data")