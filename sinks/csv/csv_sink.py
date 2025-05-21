import pandas as pd
from sinks.csv import CSVSinkBase
from utils.datastore import Datastore
from modules.bulk_insert_to_kafka  import *
from app.niceGUI.utils.show_processed_data import show_processed_data

class CSV(CSVSinkBase):
    """Class for sinking data into CSV."""
    def __init__(self, callback_function=None, kwargs=None):
        super().__init__("sink_data_synthetic.csv", callback_function, kwargs)

    def process_fetched_data(self, products, start_datetime, end_datetime):
        if Datastore.filtered_df.empty:
            return

        try:
            # ✅ Step 1: Dummy Processing (Modify as needed)
            processed_df = Datastore.filtered_df.copy()
            processed_df["Processed_Column"] = processed_df["Price"]*2  # Example transformation

            # ✅ Step 2: Store Processed Data in Dictionary (Key = "processed_result")
            sink_name = self.__class__.__name__  
            Datastore.processed_dict[sink_name] = processed_df 
            print(f'Dictionary {Datastore.processed_dict}')

            # ✅ Step 3: Update UI
            show_processed_data()
            return super().process_fetched_data(products, start_datetime, end_datetime)

        except Exception as e:
            return False, str(e)  
        

    def dump_data_to_sink(self):
        """Sinks processed data into Kafka only if callback_function returns True."""
        processed_dfs = Datastore.processed_dict  # ✅ Get stored processed data
        if not processed_dfs:
            return False, "No data to sink"
        try:
            sink_name = self.__class__.__name__ 
            for name, df in processed_dfs.items():
                if (name==sink_name):
                    # ✅ Step 3: Sink data into Kafka
                    df.to_csv(self.file, header=False)
            return super().dump_data_to_sink()
        except Exception as e:
            print(f"❌ Sink Error: {e}")
            return False, str(e) 
 
