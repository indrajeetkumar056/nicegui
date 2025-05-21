import asyncio
import pandas as pd
import io
from nicegui import ui
from concurrent.futures import ThreadPoolExecutor
from configs import *
from utils.helper import fetch_products_from_instrument_url

executor = ThreadPoolExecutor()

def select_all(product_selector):
    """Selects all products in the product selector."""
    all_products = product_selector.options[1:]  # Exclude "Select All"
    product_selector.set_value(all_products)

def toggle_select_all(e, product_selector):
    """Handles 'Select All' selection."""
    if "Select All" in e.value:
        loop1 = asyncio.get_event_loop()
        loop1.run_in_executor(executor, lambda: select_all(product_selector))


def update_products(value, product_selector,upload_event=None):
    """Updates product options based on selected product type."""
    if value in PRODUCTS_CRITERIA:
        products = fetch_products_from_instrument_url(PRODUCTS_CRITERIA[value])
        products.insert(0, "Select All")
        product_selector.set_options(products)
    elif value == 'CSV/Parquet' and upload_event:
        handle_file_upload(upload_event, product_selector)
    else:
        product_selector.set_options([])


def handle_file_upload(event, product_selector):
    """Handles file upload and updates the product selector with unique product values."""
    try:
        file = event.content.read()
        file_ext = event.name.split('.')[-1].lower()

        if file_ext == 'csv':
            df = pd.read_csv(io.BytesIO(file))
        elif file_ext in ['parquet', 'pq']:
            df = pd.read_parquet(io.BytesIO(file))
        else:
            ui.notify('Invalid file format. Please upload a CSV or Parquet file.', type='negative')
            return

        if 'product' in df.columns:
            products = df['product'].dropna().unique().tolist()
            products.insert(0, "Select All")
            product_selector.set_options(products)
            ui.notify(f"Loaded {len(df)} products from file.", type='positive')
        else:
            ui.notify("No 'product' column found in file.", type='negative')
            product_selector.set_options([])

    except Exception as e:
        ui.notify(f"Error reading file: {str(e)}", type='negative')


def on_product_type_change(e,file_upload,product_selector):
    if e.value == 'CSV/Parquet':
        file_upload.set_visibility(True)
    else:
        file_upload.set_visibility(False)
        update_products(e.value, product_selector)
