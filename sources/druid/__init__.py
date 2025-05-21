import pandas as pd
from sources import SourceBase  
import json
import requests
import time
import pandas as pd
from configs import * 
from utils.helper import get_synthetic_inst

class DruidSourceBase(SourceBase):
    def __init__(self, table, start_date, end_date, products):
        super().__init__(start_date, end_date, products)
        self.table = table

    def filter_products(self):
        expanded_products = set()
        synthetic_instruments = get_synthetic_inst()
        for product in self.products:
            if product in synthetic_instruments:
                expanded_products.update(synthetic_instruments[product])  # Add legs instead
            else:
                expanded_products.add(product)

        return list(expanded_products)

    def fetch_data(self):
        """Common fetch logic for all Druid sources"""
        start_datetime = pd.to_datetime(self.start_date, errors='coerce')
        end_datetime = pd.to_datetime(self.end_date, errors='coerce')

        iso_format = lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        start_time, end_time = iso_format(start_datetime), iso_format(end_datetime)
        print("Fetching from source", DRUID_DATASOURCE)

        expanded_instruments = self.filter_products()
        print(expanded_instruments)
        instruments_str = ", ".join([f"'{instrument}'" for instrument in expanded_instruments])
        last_time = start_time
        all_data = []
    
        while True:
            query = {
                "query": f"""
                    SELECT __time, source, instrument, price, qty, side
                    FROM {DRUID_DATASOURCE} 
                    WHERE instrument IN ({instruments_str}) 
                    AND __time >= '{last_time}' AND __time <= '{end_time}'
                    ORDER BY __time ASC
                    LIMIT {DRUID_BATCH_SIZE}
                """,
                "context": {
                    "maxSubqueryRows": 10_000_000,
                    "maxSubqueryBytes": "auto",
                    "sqlOuterLimit": 10_000_000,
                    "useCache": True
                }
            }

            for attempt in range(DRUID_RETRIES):
                try:
                    response = requests.post(f"{DRUID_OVERLORD_URL}/druid/v2/sql", json=query, verify=False)
                    response.raise_for_status()

                    batch_data = response.json()
                    if not batch_data:  # Stop when no more data is available
                        print("No more data to fetch from Druid.")
                        return pd.DataFrame(all_data)

                    df = pd.DataFrame(batch_data)
                    all_data.extend(batch_data)

                    # Update last_time to fetch the next batch
                    last_time = df["__time"].max()

                    # If batch size is smaller than requested, assume all data is fetched
                    if len(df) < DRUID_BATCH_SIZE:
                        print("Fetched fewer rows than batch size, assuming all data is retrieved.")
                        return pd.DataFrame(all_data)

                    break  # Exit retry loop if successful

                except requests.exceptions.RequestException as e:
                    print(f"Attempt {attempt + 1} failed with error: {e}")
                    if attempt < DRUID_RETRIES - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    else:
                        print(f"Max retries reached. Error: {e}")
                        return pd.DataFrame()

                except Exception as e:
                    print(f"Unexpected error while fetching TAS data from Druid: {e}")
                    return pd.DataFrame()