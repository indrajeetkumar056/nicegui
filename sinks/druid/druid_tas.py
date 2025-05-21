from sinks.druid import DruidSinkBase
from utils.datastore import Datastore
import pandas as pd
from datetime import datetime, timezone
from configs import DRUID_DATASOURCE
from app.niceGUI.utils.show_processed_data import show_processed_data
from modules.raw_ticks_correction_druid import reindex_druid_data, start_http_server, backfill_druid_with_http, monitor_druid_task, fetch_task_logs

class DruidTasSink(DruidSinkBase):
    """Sink class for Kafka topic."""

    def __init__(self, callback_function=None, kwargs=None):
        super().__init__(DRUID_DATASOURCE, callback_function, kwargs)

    def process_fetched_data(self, products, start_datetime, end_datetime):
        """Process and modify data before sending to Kafka."""
        # ✅ Step 1: Copy the filtered DataFrame
        if Datastore.filtered_df.empty:
            return
        processed_df = Datastore.filtered_df.copy()
        print(f"Processing data for {self.table}")

        # ✅ Step 3: Processing df
        required_columns = {"Timestamp", "Instrument", "Price", "Qty"}
        missing_columns = required_columns - set(processed_df.columns)
        if missing_columns:
            print(f"Error: Missing columns: {missing_columns}")
            return
        processed_df = processed_df[list(required_columns)]
        print(processed_df)

        print("Reindexing Druid data to exclude specified products...")
        iso_format = lambda ts: datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        start_datetime, end_datetime = iso_format(processed_df["Timestamp"].min()), iso_format(processed_df["Timestamp"].max())
        processed_df.columns = processed_df.columns.str.lower()
        dimensions = [col for col in processed_df.columns if col != "timestamp"]
        reindex_druid_data(start_datetime, end_datetime, products, dimensions)

        sink_name = self.__class__.__name__ 
        final_dict = {}
        final_dict[sink_name] = processed_df 
        Datastore.processed_dict = final_dict  
        print(f'Dictionary {Datastore.processed_dict}')

        # ✅ Step 3: Update UI
        show_processed_data()
        return super().process_fetched_data(products, start_datetime, end_datetime)
        
    def dump_data_to_sink(self):
        """Dump processed data to DB."""
        print(f"Dumping processed data to {self.table}")
        processed_dfs = Datastore.processed_dict  # ✅ Get stored processed data

        if not processed_dfs:
            return False, "No data to sink"
        try:
            sink_name = self.__class__.__name__ 
            for name, df in processed_dfs.items():
                if (name==sink_name):
                    csv_file = "backfill_data.csv"
                    df.to_csv(csv_file, index=False)
                    print(f"Kafka messages dumped to CSV file: {csv_file}")

                    print("Starting HTTP server to serve the CSV file...")
                    httpd, http_url = start_http_server(directory=".")
                    http_file_url = f"{http_url}/{csv_file}"

                    print(f"Backfilling Druid data from HTTP URL: {http_file_url}")
                    task_id = backfill_druid_with_http(file_url=http_file_url)

                    if task_id:
                        task_status = monitor_druid_task(task_id)
                        print(f"Druid task completed with status: {task_status}")
                        if task_status == "FAILED":
                            print("Fetching logs for the failed task...")
                            fetch_task_logs(task_id)
                    print("Stopping HTTP server...")
                    httpd.shutdown()
            return super().dump_data_to_sink()
        except Exception as e:
            print(f"❌ Sink Error: {e}")
            return False, str(e) 
        
