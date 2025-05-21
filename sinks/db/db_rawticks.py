import pandas as pd
from sinks.db import DBSinkBase
from utils.datastore import Datastore
from configs import RAW_DATA_DB_NAME, RAW_TICKS_TABLE_NAME_PROD, RAW_TICKS_TABLE_NAME_DEV
from app.niceGUI.utils.show_processed_data import show_processed_data
from modules.delete_records_from_db import delete_records_from_db
from modules.bulk_insert_to_db import bulk_insert_into_db
from utils.helper import get_env

class DBRawTicksSink(DBSinkBase):
    """Sink class for DB."""

    def __init__(self, callback_function=None, kwargs=None):
        super().__init__(RAW_DATA_DB_NAME, callback_function, kwargs)

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process and modify data before sending to Kafka."""
        if Datastore.filtered_df.empty:
            return 
        try:
            # ✅ Step 1: Copy the filtered DataFrame
            processed_df = Datastore.filtered_df.copy()
            print(f"Processing data for {self.db}")
            
            # ✅ Step 2: Deleting existing data
            print(f"Deleting data from {self.db}")
            start_ms = int(pd.Timestamp(start_datetime).timestamp() * 1000)
            end_ms = int(pd.Timestamp(end_datetime).timestamp() * 1000)
            table = get_env(self.kwargs, RAW_TICKS_TABLE_NAME_PROD, RAW_TICKS_TABLE_NAME_DEV)
            # delete_records_from_db(self.db, table, products, start_ms, end_ms)

            # ✅ Step 3: Processing df
            sink_name = self.__class__.__name__  
            final_dict = {}
            processed_df["Timestamp"] = processed_df["Timestamp"].astype(int)
            processed_df = processed_df[(processed_df["Timestamp"] >= start_ms) & (processed_df["Timestamp"] <= end_ms)]
            processed_df = processed_df[["Timestamp", "Source", "Instrument", "Price", "Qty", "Side"]]
            final_dict[sink_name] = processed_df 
            Datastore.processed_dict = final_dict  
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
            table = get_env(self.kwargs, RAW_TICKS_TABLE_NAME_PROD, RAW_TICKS_TABLE_NAME_DEV)

            for name, df in processed_dfs.items():
                if (name==sink_name):
                    bulk_insert_into_db(self.db, table, df)
            return super().dump_data_to_sink()
        except Exception as e:
            print(f"❌ Sink Error: {e}")
            return False, str(e) 