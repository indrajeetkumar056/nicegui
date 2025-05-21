import pandas as pd
from sources.kafka import KafkaSourceBase
from utils.datastore import Datastore
from configs import KAFKA_TOPIC_MAIN, SCHEMA_REGISTRY_URL
from utils.helper import get_mapping

class KafkaTasSource(KafkaSourceBase):
    """Class for handling kafka-based data fetch"""
    def __init__(self, start_date, end_date, products):
        super().__init__(KAFKA_TOPIC_MAIN, start_date, end_date, products, SCHEMA_REGISTRY_URL)

    def fetch_data(self):
        """Fetch data from kafka based on filters."""
        try:
            mapping_df = get_mapping()
            df = super().fetch_data()
            
            if df.empty:
                return df, "No data fetched", False
            
            df["InstrumentId"] = df["InstrumentId"].astype(str)
            df = df[(df["IsOtc"] == False) & (df["IsLegTrade"] == False)]
            df = df.merge(mapping_df, on="InstrumentId", how="left", suffixes=('', '_mapping'))
            df["Side"] = df["Direction"].apply(lambda x: -1 if x == "Hit" else (1 if x == "Take" else 0))
            df["Source"] = 'tas' 
            df = df.sort_values("Timestamp")
            filtered_df = df[["Timestamp", "Source", "Instrument", "Price", "Qty", "Side"]]
            filtered_df = super().filter_data(filtered_df)
            Datastore.filtered_df = filtered_df
           
            return filtered_df, "Data fetched successfully", True
        except Exception as e:
            return pd.DataFrame(), str(e), False

