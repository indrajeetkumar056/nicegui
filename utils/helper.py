import pandas as pd
import requests
from datetime import datetime
import pytz
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
requests = requests.Session()
requests.verify = False

def fetch_products_from_instrument_url(product_url):
    try:
        response=requests.get(product_url)
        data=response.json()
        return [item['quant_code'] for item in data.get('data',[])]
    except Exception as e:
        print(f"Error fetching products:{e}")

def get_start_of_day_timestamp():
    """Returns the UNIX timestamp for the start of the current day in UTC."""
    now = datetime.now(pytz.timezone("Europe/London"))
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(start_of_day.timestamp() * 1000)  

def create_dataframe_from_buffer(buffer):
    try:
        data = []
        for message in buffer:
            parts = message.split(',')
            timestamp = int(parts[0])
            product = parts[2]
            price = float(parts[3])
            quantity = float(parts[4])
            data.append({'timestamp': timestamp, 'product': product, 'price': price, 'quantity': quantity})

        return pd.DataFrame(data)

    except Exception as e:
        print(f"Error creating DataFrame from buffer: {e}")
        return pd.DataFrame()

def extract_products_from_buffer(buffer):
    products = set()
    for record in buffer:
        try:
            parts = record.split(',')
            products.add(parts[2])
        except Exception as e:
            print(f"Error extracting product from record: {record}, error: {e}")
    return list(products)

def get_timestamp_and_uuid_from_message(message):
    try:
        parts = message.split(',')
        return (parts[0]), (parts[1])  
    except Exception as e:
        print(f"Error extracting timestamp from message: {message}, error: {e}")
        return None

import os 
from configs import *

def get_available_sources():
    source_files = []
    for category in os.listdir(SOURCE_DIR):
        category_path = os.path.join(SOURCE_DIR, category)
        if os.path.isdir(category_path):
            files = [f"{f[:-3]}" for f in os.listdir(category_path) if f.endswith(".py") and f != "__init__.py"]
            source_files.extend(files)
    return source_files

def get_available_sinks():
    sink_files = []
    for category in os.listdir(SINK_DIR):
        category_path = os.path.join(SINK_DIR, category)
        if os.path.isdir(category_path):
            files = [f"{f[:-3]}" for f in os.listdir(category_path) if f.endswith(".py") and f != "__init__.py"]
            sink_files.extend(files)
    return sink_files

# from configs import PRODUCTS_CRITERIA
import asyncio
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor()

def fetch_products(url):
    try:
        response=requests.get(url,verify=False)
        data=response.json()
        return[item['quant_code'] for item in data.get('data',[])]
    except Exception as e:
        print(f"Error fetching products:{e}")


def get_mapping():
    response = requests.get(TT_INSTRUMENT_URL)
    data = response.json()
    instrument_product_mapping = {item['tt_instrument_id']: item['quant_code'] for item in data['data']}
    mapping_df = pd.DataFrame(list(instrument_product_mapping.items()), columns=["InstrumentId", "Instrument"])

    return mapping_df

import pandas as pd

def parse_kwargs(kwargs_str):
    """Parses query string parameters into a dictionary."""
    if not kwargs_str:  # ✅ Handle empty or None case
        return {}

    parsed_kwargs = {}
    for param in kwargs_str.split("&"):
        key, value = param.split("=")
        parsed_kwargs[key] = value.split(",")  # Convert CSV values into list
    return parsed_kwargs

def callback_function(df):
    """Verifies if the dataframe is empty or not."""
    try:
        if df.empty:
            return False, "Data is empty"
        return True, "Data verification passed"
    except Exception as e:
        return False, str(e)


