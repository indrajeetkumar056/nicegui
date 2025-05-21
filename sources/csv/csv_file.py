import pandas as pd
from nicegui import ui
from sources.csv import CSVSourceBase
from utils.datastore import Datastore

class CSVSource(CSVSourceBase):
    """Class for handling CSV-based data fetch"""
    def __init__(self, start_date, end_date, products):
        super().__init__('fetched_data_synthetic.csv', start_date, end_date, products)

    def fetch_data(self):
        """Fetch data from CSV based on filters."""
        try:
            df = super().fetch_data()
            print(df)
            if df.empty:
                return df, "No data fetched", False
            filtered_df = super().filter_data(df)
            Datastore.filtered_df = filtered_df
            return filtered_df, "Data fetched successfully", True
        except Exception as e:
            return pd.DataFrame(), str(e), False
