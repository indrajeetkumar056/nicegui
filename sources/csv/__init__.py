import pandas as pd
from sources import SourceBase  
from utils.helper import get_synthetic_inst

class CSVSourceBase(SourceBase):
    def __init__(self, file, start_date, end_date, products):
        super().__init__(start_date, end_date, products)
        self.file = file

    def fetch_data(self):
        """Common fetch logic for all Kafka sources"""
        
        start_datetime = pd.to_datetime(self.start_date, errors='coerce')
        end_datetime = pd.to_datetime(self.end_date, errors='coerce')
        df = pd.read_csv(self.file)
        df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms')
             
        filtered_csv = df[
                (df['Timestamp'] >= start_datetime) &
                (df['Timestamp'] <= end_datetime)
            ]
        if not filtered_csv.empty:
            filtered_csv['Timestamp'] = filtered_csv['Timestamp'].astype('int64') // 10**6  # Convert to milliseconds
        return filtered_csv
    
    def filter_data(self, df):
        if not self.products:
            return df
        expanded_products = set()
        synthetic_instruments = get_synthetic_inst()
        for product in self.products:
            if product in synthetic_instruments:
                expanded_products.update(synthetic_instruments[product])  # Add legs instead
            else:
                expanded_products.add(product)

        return df[df["Instrument"].isin(list(expanded_products))]