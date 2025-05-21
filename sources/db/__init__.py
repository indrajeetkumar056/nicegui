import io
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from configs import *
from sources import SourceBase
from utils.helper import get_synthetic_inst

class DBSourceBase(SourceBase):
    def __init__(self, table, start_date, end_date, products):
        super().__init__(start_date, end_date, products)
        self.table = table
    
    def fetch_hourly_data(self, start_ts, end_ts):
        """Fetches data for a given hourly range."""
        query = f"""
        COPY (
            SELECT * FROM {self.table}
            WHERE "Timestamp" >= {start_ts} AND "Timestamp" < {end_ts}
        ) TO STDOUT WITH CSV HEADER;
        """
        
        conn = psycopg2.connect(host=DB_HOST, dbname=RAW_DATA_DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cursor = conn.cursor()
        buffer = io.StringIO()
        cursor.copy_expert(query, buffer)
        buffer.seek(0)
        
        df = pd.read_csv(buffer)
        print(df)
        conn.close()
        return df

    def fetch_data(self, mapping_df=None):
        """Common fetch logic for all DB sources"""
        
        start_datetime = pd.to_datetime(self.start_date, errors='coerce')
        end_datetime = pd.to_datetime(self.end_date, errors='coerce')
        # Compute time difference in hours
        time_diff = (end_datetime - start_datetime).total_seconds() / 3600
        if time_diff>=12:
            if start_datetime.weekday()==0:
                yesterday = (start_datetime - timedelta(days=3)).replace(hour=7, minute=0, second=0, microsecond=0)
            else:
                yesterday = (start_datetime - timedelta(days=1)).replace(hour=7, minute=0, second=0, microsecond=0)
            start_datetime = yesterday
        unix_timestamp_from = int(start_datetime.timestamp() * 1000)
        unix_timestamp_to = int(end_datetime.timestamp() * 1000)

        print(f"Fetching data from {self.table}: {unix_timestamp_from} to {unix_timestamp_to}")

        hourly_intervals = [(int((start_datetime + pd.Timedelta(hours=i)).timestamp() * 1000),
                         int((start_datetime + pd.Timedelta(hours=i + 1)).timestamp() * 1000))
                        for i in range(int(time_diff))]

        # Parallel execution
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = executor.map(lambda x: self.fetch_hourly_data(*x), hourly_intervals)

        df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

        if df.empty:
            return df

        if mapping_df is not None and not mapping_df.empty:
            df["InstrumentId"] = df["InstrumentId"].astype(str)
            df = df[(df["IsOtc"] == 'f') & (df["IsLegTrade"] == 'f')]
            df = df.merge(mapping_df, on="InstrumentId", how="left", suffixes=('', '_mapping'))
            df["Side"] = df["Direction"].apply(lambda x: -1 if x == "Hit" else (1 if x == "Take" else 0))
            df["Source"] = 'tas' 

        filtered_df = self.filter_data(df)

        filtered_df = filtered_df[["Timestamp", "Source", "Instrument", "Price", "Qty", "Side"]]

        return filtered_df
    
    def filter_data(self, df):
        expanded_products = set()
        synthetic_instruments = get_synthetic_inst()
        for product in self.products:
            if product in synthetic_instruments:
                expanded_products.update(synthetic_instruments[product])  # Add legs instead
            else:
                expanded_products.add(product)

        return df[df["Instrument"].isin(list(expanded_products))]
