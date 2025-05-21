from sinks.kafka import KafkaSinkBase
from utils.datastore import Datastore
import pandas as pd
from configs import KAFKA_TOPIC_RAW_TICKS_TEST
from app.niceGUI.utils.show_processed_data import show_processed_data
from modules.bulk_insert_to_kafka import bulk_insert_ohlc_data_kafka

class KafkaDeserializedSink(KafkaSinkBase):
    """Sink class for Kafka topic."""

    def __init__(self, callback_function=None, kwargs=None):
        super().__init__(KAFKA_TOPIC_RAW_TICKS_TEST, callback_function, kwargs)

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process and modify data before sending to Kafka."""
        # ✅ Step 1: Copy the filtered DataFrame
        if Datastore.filtered_df.empty:
            return
        processed_df = Datastore.filtered_df.copy()
        print(f"Processing data for {self.topic}")

        start_ms = int(pd.Timestamp(start_datetime).timestamp() * 1000)
        end_ms = int(pd.Timestamp(end_datetime).timestamp() * 1000)

        # ✅ Step 3: Processing df
        sink_name = self.__class__.__name__  
        final_dict = {}
        processed_df["Timestamp"] = processed_df["Timestamp"].astype(int)
        processed_df = processed_df[(processed_df["Timestamp"] >= start_ms) & (processed_df["Timestamp"] <= end_ms)]
        processed_df = processed_df[["Key", "Timestamp", "Source", "Instrument", "Price", "Qty", "Side"]]
        final_dict[sink_name] = processed_df 
        Datastore.processed_dict = final_dict  
        print(f'Dictionary {Datastore.processed_dict}')

        # ✅ Step 3: Update UI
        show_processed_data()
        return super().process_fetched_data(products, start_datetime, end_datetime)
        
    def dump_data_to_sink(self):
        """Dump processed data to DB."""
        print(f"Dumping processed data to {self.topic}")
        processed_dfs = Datastore.processed_dict  # ✅ Get stored processed data

        if not processed_dfs:
            return False, "No data to sink"
        try:
            sink_name = self.__class__.__name__ 
            for name, df in processed_dfs.items():
                if (name==sink_name):
                    bulk_insert_ohlc_data_kafka(self.topic, df)
            return super().dump_data_to_sink()
        except Exception as e:
            print(f"❌ Sink Error: {e}")
            return False, str(e) 
        
