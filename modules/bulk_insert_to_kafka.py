from kafka import KafkaProducer
import json 
from configs import *
import pandas as pd

def bulk_insert_ohlc_data_kafka(backfill_topic_name, df, table=''):
    try:
        # ✅ Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            key_serializer=str.encode,
            value_serializer=lambda v: v.encode('utf-8')   
        )

        for _, row in df.iterrows():
            key = str(row["Key"] if "Key" in df.columns else table)
            value_fields = [str(row[col]) for col in df.columns if col not in ["Key"]]  
            
            value = ",".join(value_fields)  # ✅ Convert list to a single string

            # ✅ Send message to Kafka
            producer.send(backfill_topic_name, key=key, value=value)

        producer.flush()  # Ensure all messages are sent
        print(f"✅ Successfully sent {len(df)} records to Kafka topic {backfill_topic_name}.")
    except Exception as e:
        print(f"❌ Error while sending data to Kafka topic {backfill_topic_name}: {e}")
    finally:
        producer.close()

