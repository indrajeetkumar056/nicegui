import pandas as pd
from urllib.parse import parse_qs
from sinks.kafka import KafkaSinkBase
from utils.datastore import Datastore
from app.niceGUI.utils.show_processed_data import show_processed_data
from modules.bulk_insert_to_kafka import bulk_insert_ohlc_data_kafka
from modules.tas_ohlcv_intraday_backfilling import process_tas_chunk_df
from modules.tas_ohlcv_daily_backfilling import  process_tas_daily_chunk_df
from configs import KAFKA_TOPIC_CANDLES_BACKFILL, OHLC_TABLE_MAPPING

class KafkaTasOHLCSink(KafkaSinkBase):
    """Class for sinking data into Kafka."""
    def __init__(self, callback_function=None, kwargs=None):
        if isinstance(kwargs, str):  # Convert query string to dict
            kwargs = {key: val[0] if len(val) == 1 else val for key, val in parse_qs(kwargs).items()}
        super().__init__(KAFKA_TOPIC_CANDLES_BACKFILL, callback_function, kwargs)

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process and modify data before sending to Kafka."""
        if Datastore.filtered_df.empty:
            return 
        try:
            # ✅ Step 1: Copy the filtered DataFrame
            processed_df = Datastore.filtered_df.copy()
            print(f"Processing data for {self.topic}")
            timeframes = []
            if self.kwargs:
                timeframes = self.kwargs["x"].split(",") if isinstance(self.kwargs["x"], str) else self.kwargs["x"]
            
            # ✅ Step 2: Process and store Processed Data
            sink_name = self.__class__.__name__
            if any(t in timeframes for t in ['1', '5', '60']):
                final_dict = process_tas_chunk_df(self.topic, processed_df, timeframes, start_datetime, end_datetime)
                Datastore.processed_dict[sink_name] = final_dict
            if "1440" in timeframes:
                final_dict = process_tas_daily_chunk_df(self.topic, processed_df, '1D', products, start_datetime, end_datetime)
                if sink_name in Datastore.processed_dict:
                    Datastore.processed_dict[sink_name].update(final_dict) 
                else:
                    Datastore.processed_dict[sink_name] = final_dict  

            print(f'Dictionary {Datastore.processed_dict}')

            # ✅ Step 3: Update UI
            show_processed_data()
            
            return super().process_fetched_data(products, start_datetime, end_datetime)

        except Exception as e:
            return False, str(e)
        
    def dump_data_to_sink(self):
        """Sinks processed data into Kafka"""
        print(f"Dumping processed data to {self.topic}")
        processed_dfs = Datastore.processed_dict  # ✅ Get stored processed data

        if not processed_dfs:
            return False, "No data to sink"
        try:
            sink_name = self.__class__.__name__ 
            for name, data_dict in processed_dfs.items():
                if (name==sink_name):
                    for timeframe, df in data_dict.items():
                        table = OHLC_TABLE_MAPPING[timeframe]
                        df["time"] = pd.to_datetime(df["time"]).astype(int) // 10**6
                        required_columns = ["time", "product", "open", "high", "low", "close", "volume", "buyvolume", "sellvolume"]
                        for col in ["buyvolume", "sellvolume"]:
                            if col not in df.columns:
                                df[col] = 0 
                        df = df[required_columns]
                        bulk_insert_ohlc_data_kafka(self.topic, df, table)
            return super().dump_data_to_sink()
        except Exception as e:
            print(f"❌ Sink Error: {e}")
            return False, str(e)