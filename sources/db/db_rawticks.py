import pandas as pd
from sources.db import DBSourceBase
from utils.datastore import Datastore
from configs import RAW_TICKS_TABLE_NAME_PROD

class DbRawTicksSource(DBSourceBase):
    """Class for handling DB-based data fetch"""
    def __init__(self, start_date, end_date, products):
        super().__init__(RAW_TICKS_TABLE_NAME_PROD, start_date, end_date, products)
    
    def fetch_data(self):
        """Fetch data from DB based on filters."""
        try:
            filtered_df = super().fetch_data()
            Datastore.filtered_df = filtered_df
            return filtered_df, "Data fetched successfully", True
        except Exception as e:
            return pd.DataFrame(), str(e), False
        

