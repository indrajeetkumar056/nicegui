from sources.kafka import KafkaSourceBase
import pandas as pd
from utils.datastore import Datastore
from configs import KAFKA_TOPIC_RAW_TICKS

class KafkaRawTicksSource(KafkaSourceBase):
    """Class for handling Kafka-based data fetch"""
    def __init__(self, start_date, end_date, products):
        super().__init__(KAFKA_TOPIC_RAW_TICKS, start_date, end_date, products)

    def fetch_data(self):
        """Fetch data from kafka based on filters."""
        try:
            df = super().fetch_data()
            if df.empty:
                return df, "No data fetched", False
            filtered_df = super().filter_data(df)
            Datastore.filtered_df = filtered_df
            return filtered_df, "Data fetched successfully", True
        except Exception as e:
            return pd.DataFrame(), str(e), False


