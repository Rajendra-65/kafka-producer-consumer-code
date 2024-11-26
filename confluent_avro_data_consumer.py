import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '7T3V6SOL74TXYCOB',
    'sasl.password': '6jtZewmxHWOpWRtPXgVoIPA7DpCDN9GgnSpF3Gcbm2Ni9pyss4/Mvvx7gcqvUNhA',
    'group.id': 'group2',
    'auto.offset.reset': 'latest'
}

# 'group.id': 'group1',

# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-lzvd0.ap-southeast-2.aws.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('7Y5JR4JDXR4DJMVH', 'ueQ3dPMKrqpZP0EoaLmST5pk/KWqlR1bd3px4fsZkbhBRC+if57/3BSU931Coymu')
})

# Fetch the latest Avro schema for the value
subject_name = 'retail-data-test-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['retail-data-test'])

#Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0) # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

# print('hello world')