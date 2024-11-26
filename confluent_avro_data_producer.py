import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time


from confluent_kafka import SerializingProducer #is a class defined to serialize producer object
from confluent_kafka.schema_registry import SchemaRegistryClient #making an coneection to registry
from confluent_kafka.schema_registry.avro import AvroSerializer # Serializig ' value' data in avro format
from confluent_kafka.serialization import StringSerializer # for serializing the' key 'as well in our case (string)
import pandas as pd


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',#host_url of kafka cluster
    'sasl.mechanisms': 'PLAIN', # secured authentication between kafka and application
    'security.protocol': 'SASL_SSL', # same as above
    'sasl.username': '7T3V6SOL74TXYCOB', # act like username (api key)
    'sasl.password': '6jtZewmxHWOpWRtPXgVoIPA7DpCDN9GgnSpF3Gcbm2Ni9pyss4/Mvvx7gcqvUNhA' # secret key of cluster
}

# Create a Schema Registry client

schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-lzvd0.ap-southeast-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('7Y5JR4JDXR4DJMVH', 'ueQ3dPMKrqpZP0EoaLmST5pk/KWqlR1bd3px4fsZkbhBRC+if57/3BSU931Coymu') #contains url and authentication schema registry
})

# Fetch the latest Avro schema for the value
subject_name = 'retail-data-test-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')

key_serializer = StringSerializer('utf_8') #tackling alphanumeric character
avro_serializer = AvroSerializer(schema_registry_client, schema_str) #

# Define the SerializingProducer



producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})



# Load the CSV data into a pandas DataFrame
df = pd.read_csv('retail_data.csv')
df = df.fillna('null')
# print(df.head(20))

# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    value = row.to_dict()
    # print(value)
    # Produce to Kafka
    producer.produce(topic='retail-data-test', key=str(index), value=value, on_delivery=delivery_report)
    producer.flush()
    time.sleep(2)
    #break

print("All Data successfully published to Kafka")

# print('hello world')