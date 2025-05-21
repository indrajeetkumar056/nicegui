from sinks.db import DBSinkBase
from utils.datastore import Datastore
from nicegui import ui
from configs import *
from app.niceGUI.utils.show_processed_data import show_processed_data
from modules.vap_daily_backfilling import process_vap_daily_chunk_async
from modules.vap_intraday_backfilling import  process_vap_chunk_async
from urllib.parse import parse_qs
from modules.bulk_insert_to_db import bulk_insert_into_db

class DBVapSink(DBSinkBase):
    """Sink class for DB."""

    def __init__(self, callback_function=None, kwargs=None):
        db_name = OHLC_DB_NAME_PROD
        env_value = None
        if isinstance(kwargs, str):  # Convert query string to dict
            kwargs = {key: val[0] if len(val) == 1 else val for key, val in parse_qs(kwargs).items()}
            env_value = kwargs.get("env")
        if env_value and env_value.lower() == "dev":
            db_name = OHLC_DB_NAME_DEV
        super().__init__(db_name, callback_function, kwargs)

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process and modify data before sending to Kafka."""
        if Datastore.filtered_df.empty:
            return 
        try:
            # ✅ Step 1: Copy the filtered DataFrame
            processed_df = Datastore.filtered_df.copy()
            print(f"Processing data for {self.db}")
            timeframes = []
            if self.kwargs:
                timeframes = self.kwargs["x"].split(",") if isinstance(self.kwargs["x"], str) else self.kwargs["x"]
            
            # ✅ Step 2: Process and store Processed Data
            sink_name = self.__class__.__name__
            if any(t in timeframes for t in ['1', '5', '60']):
                final_dict = process_vap_chunk_async(self.db, processed_df, timeframes, start_datetime, end_datetime)
                Datastore.processed_dict[sink_name] = final_dict
            if "1440" in timeframes:
                final_dict = process_vap_daily_chunk_async(self.db, processed_df, '1D', products, start_datetime, end_datetime)
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
        """Dump processed data to DB."""

        print(f"Dumping processed data to {self.db}")
        processed_dfs = Datastore.processed_dict  # ✅ Get stored processed data

        if not processed_dfs:
            return False, "No data to sink"
        try:
            sink_name = self.__class__.__name__ 
            for name, data_dict in processed_dfs.items():
                if (name==sink_name):
                    for timeframe, df in data_dict.items():
                        table = VAP_TABLE_MAPPING[timeframe]
                        bulk_insert_into_db(self.db, table, df)
            return super().dump_data_to_sink()
        except Exception as e:
            print(f"❌ Sink Error: {e}")
            return False, str(e) 
        
