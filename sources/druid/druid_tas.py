import pandas as pd
from sources.druid import DruidSourceBase
from utils.datastore import Datastore
from configs import DRUID_DATASOURCE

class DruidTasSource(DruidSourceBase):
    """Class for handling Druid-based data fetch"""
    def __init__(self, start_date, end_date, products):
        super().__init__(DRUID_DATASOURCE, start_date, end_date, products)

    def fetch_data(self):
        """Fetch data from DB based on filters."""
        try:
            filtered_df = super().fetch_data()
            filtered_df["__time"] = pd.to_datetime(filtered_df["__time"]).astype(int) // 10**6  
            filtered_df.rename(columns={"__time": "Timestamp", "source": "Source", "instrument": "Instrument", "price": "Price", "qty": "Qty", "side": "Side"}, inplace=True)
            Datastore.filtered_df = filtered_df
            return filtered_df, "Data fetched successfully", True
        except Exception as e:
            return pd.DataFrame(), str(e), False

   