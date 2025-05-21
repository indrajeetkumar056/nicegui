import requests
import urllib3
from kafka import KafkaProducer
import psycopg2
from psycopg2 import extras
import pandas as pd
from decimal import Decimal
from datetime import timedelta

# Suppress warnings for verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from configs import *

#%%

# Extracting lag-instrument list
response = requests.get(INTERMARKET_OUTPUT_URL, verify=False)
data = response.json()
synthetic_df = pd.json_normalize(data['data'])

response = requests.get(REUTERS_INSTRUMENT_URL, verify=False)
data2 = response.json()
reuters_df = pd.json_normalize(data2['data'])

reuters_codes = reuters_df['QH_Code'].tolist()

formula_qh_code_map = {}
qh_code_legs_map = {}

# Iterate over each row
for index, row in synthetic_df.iterrows():
    legs = row['Legs'].split(',')  
    mults = row['Mult'].split(',')  
    qh_code = row['QH_Code']
    
    # Ensure there are equal number of legs and multipliers
    if len(legs) != len(mults):
        raise ValueError(f"Row {index} has mismatched legs and multipliers.")
    
    # Qhcode-Legs mapping
    qh_code_legs_map[qh_code] = legs

    # Create the formula by pairing legs with their corresponding multipliers
    formula = " + ".join([f"{leg}*{mult}" for leg, mult in zip(legs, mults)])

    # Formula-Qhcode mapping    
    if formula not in formula_qh_code_map:
        formula_qh_code_map[formula] = []
    formula_qh_code_map[formula].append(qh_code)

tick_size_mapping = {entry['QH_Code']: entry['Tick_size'] for entry in synthetic_df.to_dict('records')}

#%%

def bulk_insert_ohlc_data_db(table_name, ohlc_df):
    try:
        # Assuming you are using psycopg2 for PostgreSQL
        print(f"Inserting in {table_name}")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=OHLC_DB_NAME_PROD,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        tuples = [(
            row['time'], row['product'], row['open'], row['high'], row['low'], row['close'], row['volume']
        ) for index, row in ohlc_df.iterrows()]
        
        insert_query = f"""
        INSERT INTO {table_name} (time, product, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (time, product) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
        """
        with conn.cursor() as cur:
            extras.execute_values(cur, insert_query, tuples)
            conn.commit()
        print(f"Successfully inserted {len(ohlc_df)} records in {table_name}.")

    except Exception as e:
        print(f"Error during bulk insert: {e}")
    finally:
        cursor.close()
        conn.close()


def bulk_insert_ohlc_data_kafka(backfill_topic_name, table_name, ohlc_df):
    try:
        # Initialize Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: v.encode('utf-8')
        )

        for _, row in ohlc_df.iterrows():
            # Format the row as a string
            message = f"{row['time'].timestamp() * 1000:.0f},{row['product']},{row['open']},{row['high']},{row['low']},{row['close']},{row['volume']},0,0"

            # Send the message with table_name as the key
            producer.send(backfill_topic_name, key=table_name, value=message)

        producer.flush()  # Ensure all messages are sent
        print(f"Successfully sent {len(ohlc_df)} records to Kafka topic {backfill_topic_name} with key '{table_name}'.")

    except Exception as e:
        print(f"Error while sending data to Kafka topic {backfill_topic_name}: {e}")

    finally:
        producer.close()

def adjust_weekend_days(timestamp):
    """
    Adjust timestamps to combine Friday and Saturday into Friday's candle
    and Sunday and Monday into Monday's candle.
    """
    weekday = timestamp.weekday()
    if weekday == 5:  # Saturday
        return timestamp.replace(hour=23, minute=59, second=59) - pd.Timedelta(days=1)  # Move to Friday 23:59:59
    elif weekday == 6:  # Sunday
        return timestamp.replace(hour=0, minute=0, second=0) + pd.Timedelta(days=1)  # Move to Monday 00:00:00
    else:
        return timestamp

def calculate_ohlcv(df, timeframes):
    print("Calculating ohlcv")
    # Store results for each timeframe
    ohlcv_results = {}

    if df.empty:
        return ohlcv_results
    # Ensure timestamp is the index and it's in datetime format
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    tables = [table for table, value in TIMEFRAMES.items() if str(value) in timeframes]
    selected_timeframes = [tf for tf, tbl in OHLC_TABLE_MAPPING.items() if tbl in tables]
    for timeframe in selected_timeframes:
        # Resample data based on the timeframe
        if timeframe == '1D':
            df['Timestamp'] = df['Timestamp'].apply(adjust_weekend_days)
        grouped = df.groupby('synthetic_instrument')
        resample_on = 'Timestamp'
        
        ohlcv_list = []
        for product, group in grouped:
            ohlcv = group.resample(timeframe, on=resample_on, label='right').apply({
                'fair_value': ['first', 'max', 'min', 'last'],
                'volume': 'sum'
            })
            # Flatten column names
            ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']

            # Reset index for the resulting DataFrame
            ohlcv = ohlcv.reset_index()
            ohlcv.rename(columns={'Timestamp': 'time'}, inplace=True)
            # Add product information
            ohlcv['product'] = product
            # from datetime import datetime

            # Get today's timestamp (00:00:00 of the current day)
            # today_date = pd.Timestamp(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
            # print(ohlcv)
            if timeframe == '1D':
                ohlcv['time'] = ohlcv['time'] - timedelta(days=1)
            # else:
            #     ohlcv = ohlcv[ohlcv['time'] > today_date]
            ohlcv = ohlcv.dropna()
            ohlcv_list.append(ohlcv)
            print(ohlcv)
        # Store the result
        ohlcv_results[timeframe] = pd.concat(ohlcv_list, ignore_index=True)

    return ohlcv_results

#%%

def round_to_tick_size(fair_value, tick_size):
    try:
        # Ensure tick_size is a float
        if tick_size is None:
            tick_size = 0.01
        tick_size = float(tick_size)
        
        # Get the number of decimal places of tick_size
        tick_size_decimal_places = abs(Decimal(str(tick_size)).as_tuple().exponent)
        
        # Round the fair_value to the required decimal places
        fair_value_rounded = round(float(fair_value), tick_size_decimal_places)
        
        # Use regular division and then multiply to achieve rounding
        rounded_value = round(fair_value_rounded / tick_size) * tick_size
        
        return round(rounded_value, tick_size_decimal_places)
    except Exception as e:
        print(fair_value, tick_size, e)
        return None

def calculate_fair_value(products_prices):
    product_price_map = {p["Instrument"]: (p["last_price"], p["is_updated"], p["Qty"]) for p in products_prices}
    fair_values = []

    for formula, synthetic_instruments in formula_qh_code_map.items():
        for synthetic_instrument in synthetic_instruments:
            legs = qh_code_legs_map[synthetic_instrument]
            # Check if all legs are present
            if all(leg in product_price_map for leg in legs):
                # Check if at least one leg has been updated
                if any(product_price_map[leg][1] for leg in legs):  # Check the is_updated flag
                    try:
                        current_formula = formula
                        for leg in legs:
                            current_formula = current_formula.replace(leg, str(product_price_map[leg][0]))
                        fair_value = eval(current_formula)
                        tick_size = tick_size_mapping.get(synthetic_instrument, 0.01)
                        fair_value = round_to_tick_size(fair_value, tick_size)
                        # Determine volume
                        if len(legs) > 1:
                            volume = 0  
                        else:
                            volume = product_price_map[legs[0]][2]  

                        fair_values.append({
                            "synthetic_instrument": synthetic_instrument,
                            "fair_value": fair_value,
                            "volume": volume
                        })
                    except Exception as e:
                        print(f"Error calculating fair value: {e}")
    return fair_values

def fair_value_func(df):
    distinct_ts = df['Timestamp'].unique()
    distinct_prod = df['Instrument'].unique()
    ts_prod_cross = pd.DataFrame([(times, product) for times in distinct_ts for product in distinct_prod], columns=['Timestamp', 'Instrument'])
    
    merged_df = pd.merge(ts_prod_cross, df, on=['Timestamp', 'Instrument'], how="left")
    merged_df['is_updated'] = ~(merged_df['Price'].isnull())
    merged_df["last_price"] = merged_df.groupby("Instrument")["Price"].ffill()
    merged_df = merged_df[~merged_df["last_price"].isnull()]
    
    # Group by timestamp and collect products and prices
    grouped_df = merged_df.groupby("Timestamp").apply(lambda x: x[["Instrument", "last_price", "is_updated", "Qty"]].to_dict("records")).reset_index(name="products_prices")
    # Calculate fair values
    grouped_df["fair_values"] = grouped_df["products_prices"].apply(calculate_fair_value)
    # Explode the fair values into rows
    fair_values = grouped_df.explode("fair_values").dropna(subset=["fair_values"])

    if fair_values.empty:
        print("⚠️ fair_values is empty. Returning an empty DataFrame.")
        return pd.DataFrame() 

    # Reset index before concatenating to avoid index conflicts
    fair_values_normalized = pd.json_normalize(fair_values["fair_values"]).reset_index(drop=True)
    fair_values_timestamp = fair_values["Timestamp"].reset_index(drop=True)

    # Concatenate the normalized DataFrame with the timestamp column
    fair_values = pd.concat([fair_values_timestamp, fair_values_normalized], axis=1)
    fair_values['Timestamp'] = pd.to_datetime(fair_values['Timestamp'], unit='s')
    
    return fair_values
    
#%%
def process_chunk(uuid, df, timeframes):
    """
    Process the collected messages in the buffer.
    Replace this with your actual processing logic.
    """
    try:
        print(f"Processing {len(df)} messages...")
    
        # Sort DataFrame by timestamp in ascending order
        df.sort_values(by='Timestamp', inplace=True)
        df['Timestamp'] = df['Timestamp'].astype(int) // 1000
        fair_value_df = fair_value_func(df)

        ohlcv_by_timeframe = calculate_ohlcv(fair_value_df, timeframes)

        print(f"Chunk with uuid {uuid} processed successfully.")

        return ohlcv_by_timeframe
        
    except Exception as e:
        print(f"Error processing chunk: {e}")
        raise e

#%%
def process_chunk_async(uuid, df, timeframes, start_datetime, end_datetime):
    start_ts = int(pd.to_datetime(start_datetime).timestamp() * 1000)
    end_ts = int(pd.to_datetime(end_datetime).timestamp() * 1000)
    df["Timestamp"] = df["Timestamp"].astype(int)

    # Filter the DataFrame based on Timestamp range
    df = df[(df["Timestamp"] >= start_ts) & (df["Timestamp"] < end_ts)]
    final_dict = process_chunk(uuid, df, timeframes)
    return final_dict

