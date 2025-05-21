from datetime import timedelta
import pandas as pd
from datetime import datetime, timedelta
from configs import *
from utils.helper import fetch_stimes, map_prod_to_stime

def calculate_ohlcv_by_day(df, from_date, to_date):
    # Adjusting date range
    adjusted_from_date = from_date
    adjusted_to_date = to_date

    if from_date.strftime("%H:%M:%S") == "00:00:00" and from_date.weekday() == 6:  # Sunday
        adjusted_from_date = from_date + timedelta(days=1)
    if to_date.strftime("%H:%M:%S") >= "22:00:00" and to_date.weekday() == 5:  # Saturday
        adjusted_to_date = to_date - timedelta(days=1)

    adjusted_to_date = adjusted_to_date.replace(hour=0, minute=0, second=0, microsecond=0)

    # Ensure 'Timestamp' is datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms")
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce')
    df['Side'] = pd.to_numeric(df['Side'], errors='coerce')

    if df.empty:
        return pd.DataFrame()  # Return empty DataFrame if no data available

    # Group by 'Instrument' and aggregate
    ohlcv_df = df.groupby("Instrument").agg(
        open=("Price", "first"),
        high=("Price", "max"),
        low=("Price", "min"),
        close=("Price", "last"),
        volume=("Qty", "sum"),
        buyvolume=("Qty", lambda x: x[df["Side"] == 1].sum()),   # Buy volume
        sellvolume=("Qty", lambda x: x[df["Side"] == -1].sum())  # Sell volume
    ).reset_index()
    print(ohlcv_df)
    # Set 'time' value
    time_value = adjusted_from_date if from_date.strftime("%H:%M:%S") == "00:00:00" else adjusted_to_date
    ohlcv_df["time"] = time_value
    ohlcv_df = ohlcv_df.rename(columns={"Instrument": "product"}) 
    final_df = ohlcv_df[["time", "open", "high", "low", "close", "volume", "buyvolume", "sellvolume", "product"]]

    return final_df

def calculate_ohlcv(df, timeframe, products, start_datetime, end_datetime):
    s_times = fetch_stimes()
    prod_stimes_map = map_prod_to_stime(products, s_times)
    
    num_days = (datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M") - datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M")).days
    num_days = max(num_days, 1)

    ohlcv_results = {}
    final_df = pd.DataFrame()
    ohlcv_df = pd.DataFrame()

    for i in range(num_days):
        current_date = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M") + timedelta(days=i)
        if current_date.weekday() >= 5:
            print("Saturday or Sunday")
            continue

        try:
            for s_time, prod in prod_stimes_map.items():
                stime_parsed = datetime.strptime(s_time, "%H:%M:%S").time()
                
                from_date = current_date.replace(hour=stime_parsed.hour, minute=stime_parsed.minute, second=stime_parsed.second)
                to_date = from_date + timedelta(days=1)

                if from_date.weekday() == 0:
                    if from_date.hour not in [0, 22, 23]:
                        from_date = to_date - timedelta(days=3)
                        to_date = from_date + timedelta(days=3)
                    else:
                        from_date = to_date - timedelta(days=2)
                        to_date = from_date + timedelta(days=2)
                
                if from_date.weekday() == 4 and from_date.hour in [0,22,23]:
                        to_date = to_date + timedelta(days=1)
                
                if stime_parsed.hour != 0 or stime_parsed.minute != 0:
                    from_date -= timedelta(days=1)
                    to_date -=timedelta(days=1)

                print(f"Processing from {from_date} to {to_date} for {s_time}")
                df["Timestamp"] = pd.to_numeric(df["Timestamp"], errors="coerce")
                df_filtered = df[(df["Timestamp"] >= from_date.timestamp() * 1000) & 
                    (df["Timestamp"] < to_date.timestamp() * 1000) & 
                    (df["Instrument"].isin(prod))]
                
                if df_filtered.empty:
                    print(f"No data found for {s_time} from {from_date} to {to_date}")
                    continue

                # Calculate OHLCV
                ohlcv_df = calculate_ohlcv_by_day(df_filtered, from_date, to_date)
                final_df = pd.concat([final_df, ohlcv_df], ignore_index=True)
        except Exception as e:
            print(f"An error occurred on day {i + 1}: {e}")    
    
    ohlcv_results[timeframe] = final_df
    return ohlcv_results

def process_chunk(df, timeframe, products, start_datetime, end_datetime):
    ohlcv_by_timeframe = calculate_ohlcv(df, timeframe, products, start_datetime, end_datetime)
    print(ohlcv_by_timeframe)
    return ohlcv_by_timeframe

def process_tas_daily_chunk_df(uuid, df, timeframe, products, start_datetime, end_datetime):
    print(f"Processing {uuid}")
    return process_chunk(df, timeframe, products, start_datetime, end_datetime)