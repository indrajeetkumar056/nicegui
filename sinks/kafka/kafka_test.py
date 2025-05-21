import pandas as pd
from sinks.kafka import KafkaSinkBase
from utils.datastore import Datastore
from nicegui import ui
from app.niceGUI.utils.show_processed_data import show_processed_data
from configs import KAFKA_TOPIC_RAW_TICKS_TEST
from modules.bulk_insert_to_kafka import bulk_insert_ohlc_data_kafka

class KafkaTestSink(KafkaSinkBase):
    """Class for sinking data into Kafka."""
    def __init__(self, callback_function=None, kwargs=None):
        super().__init__(KAFKA_TOPIC_RAW_TICKS_TEST, callback_function, kwargs)

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Kafka-specific processing"""
        if Datastore.filtered_df.empty:
            return
        try:
            # ✅ Step 1: Dummy Processing (Modify as needed)
            processed_df = Datastore.filtered_df.copy()
            processed_df["Processed_Column"] = processed_df["Price"] * 1.1  

            # ✅ Step 2: Store Processed Data in Dictionary (Key = "processed_result")
            sink_name = self.__class__.__name__  
            Datastore.processed_dict[sink_name] = processed_df  # Store data separately for each sink 
            print(f'Dictionary {Datastore.processed_dict}')

            # ✅ Step 3: Update UI
            show_processed_data()
            return super().process_fetched_data(products, start_datetime, end_datetime)  # ✅ Ensure return value
        except Exception as e:
            return False, str(e) 

    def dump_data_to_sink(self):
        """Send data to Kafka topic"""
        print(f"Sending data to Kafka topic: {self.topic}")
        processed_dfs = Datastore.processed_dict  

        if not processed_dfs:
            return False, "No data to sink"
        try:
            for name, df in processed_dfs.items():
                # ✅ Step 1: Validate data with callback
                try:
                    # status, message = callback_function(df)
                    # if not status:
                    #     print(f"Skipping sink for {name}: {message}")
                    #     # uni.noify(f"Skipping {name}: {message}", type="warning")
                    #     continue  # ❌ Skip sinking

                    print(f"Callback passed for {name}: Proceeding with sink...")

                except Exception as e:
                    print(f"Callback failed for {name}: {e}")
                    continue  # ❌ Skip sinking due to callback failure

                # ✅ Step 2: Adjust timestamps before sinking
                df['Timestamp'] = pd.to_datetime(df['Timestamp'])

                # ✅ Step 3: Sink data into Kafka
                # bulk_insert_ohlc_data_kafka2(KAFKA_TOPIC_CANDLES_BACKFILL, df)
            return True, "Data successfully sunk"
        except Exception as e:
            print(f"❌ Sink Error: {e}")
            # uni.noify(f"Sink failed: {str(e)}", type="error")
            return False, str(e) 