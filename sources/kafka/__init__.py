import pandas as pd
from sources import SourceBase  
from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from utils.helper import get_synthetic_inst
import json
import time
from configs import *

class KafkaSourceBase(SourceBase):
    def __init__(self, topic, start_date, end_date, products, schema_registry=None):
        super().__init__(start_date, end_date, products)
        self.topic = topic
        self.schema_registry = schema_registry

    def fetch_data(self):
        """Common fetch logic for all Kafka sources"""
        
        start_datetime = pd.to_datetime(self.start_date, errors='coerce')
        print(f"{start_datetime}")
        end_datetime = pd.to_datetime(self.end_date, errors='coerce')
        print(f"{end_datetime}")
        
        unix_timestamp_from = int(start_datetime.timestamp() * 1000)
        unix_timestamp_to = int(end_datetime.timestamp() * 1000)

        raw_data = self.fetch_raw_data(unix_timestamp_from, unix_timestamp_to)
        return self.process_fetched_data(raw_data)

    def filter_data(self, df):
        if not self.products:
            return df
        expanded_products = set()
        synthetic_instruments = get_synthetic_inst()
        for product in self.products:
            if product in synthetic_instruments:
                expanded_products.update(synthetic_instruments[product])  # Add legs instead
            else:
                expanded_products.add(product)

        return df[df["Instrument"].isin(list(expanded_products))]


    def fetch_raw_data(self, start_time, end_time):
        print("Consuming from topic:", self.topic)

        if self.schema_registry:
            schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            avro_deserializer = AvroDeserializer(schema_registry_client)

        conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': MANUAL_BACKFILL_GROUP_ID,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': False
        }

        consumer = Consumer(conf)
        consumer.subscribe([self.topic])

        # Get partition info
        partitions = consumer.list_topics(topic=self.topic).topics[self.topic].partitions.keys()
        topic_partitions = [TopicPartition(self.topic, p) for p in partitions]

        # Get the offsets for the given timestamp
        offsets = consumer.offsets_for_times([TopicPartition(self.topic, p, start_time) for p in partitions])

        valid_partitions = [
            TopicPartition(tp.topic, tp.partition, offsets[i].offset)
            for i, tp in enumerate(topic_partitions)
            if offsets[i] and offsets[i].offset != -1  
        ]

        if not valid_partitions:
            print("No valid partitions found for given timestamp.")
            return []

        consumer.assign(valid_partitions)  # Assign all at once

        data = []
        last_message_time = time.time()  # Track last message time

        while True:
            msg = consumer.poll(1.0)  # Poll messages with a timeout
            if msg is None:
                # Check if 30 seconds have passed since last message
                if time.time() - last_message_time > 30:
                    print("No messages received for 30 seconds. Exiting...")
                    break
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                break

            msg_time = msg.timestamp()[1]  # Message timestamp
            if msg_time > end_time:
                break  # Stop if end time is exceeded
            last_message_time = time.time()  # Update last message time
            raw_key = msg.key()
            key = raw_key.decode('utf-8') if raw_key else None
            value = None
            try:
                msg_value = msg.value()
                # Detect Avro (Schema Registry must be provided)
                if self.schema_registry :
                    value = avro_deserializer(msg.value(), None)
                else:
                    value = msg_value.decode('utf-8')
                data.append({"key": key, "value": value})
            except (json.JSONDecodeError, KafkaException):
                print(f"Skipping malformed message: {msg_value}")

        consumer.close()
        return data
    
    def process_fetched_data(self, raw_data):
        # Convert raw_data into a DataFrame directly
        df = pd.DataFrame.from_records(raw_data)
        df.rename(columns={"key": "Key", "value": "RawValue"}, inplace=True)

        # Detect JSON vs. CSV
        def parse_value(value):
            if isinstance(value, dict):  # Avro (JSON format)
                return value
            elif isinstance(value, str):  # CSV format
                values = value.split(",")
                if len(values) == 6:
                    columns = ["Timestamp", "Source", "Instrument", "Price", "Qty", "Side"]
                elif len(values) > 6:
                    columns = ["Timestamp", "Instrument", "Open", "High", "Low", "Close", "Volume", "BuyVolume", "SellVolume"]
                else:
                    return {}  # Handle unexpected cases

                return dict(zip(columns, values))

        # Apply parsing logic to extract fields
        df = df.join(pd.DataFrame(df["RawValue"].apply(parse_value).tolist()))
        df.drop(columns=["RawValue"], inplace=True)

        return df
