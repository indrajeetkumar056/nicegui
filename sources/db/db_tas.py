import pandas as pd
from sources.db import DBSourceBase
from utils.datastore import Datastore
from configs import TAS_TABLE_NAME
from utils.helper import get_mapping

class DbTasSource(DBSourceBase):
    """Class for handling DB-based data fetch"""
    def __init__(self, start_date, end_date, products):
        super().__init__(TAS_TABLE_NAME, start_date, end_date, products)
    
    def fetch_data(self):
        """Fetch data from DB based on filters."""
        try:
            mapping_df = get_mapping()
            filtered_df = super().fetch_data(mapping_df)
            Datastore.filtered_df = filtered_df
            return filtered_df, "Data fetched successfully", True
        except Exception as e:
            return pd.DataFrame(), str(e), False
        

