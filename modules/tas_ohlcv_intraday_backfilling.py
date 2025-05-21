import pandas as pd
from configs import *

def calculate_ohlcv(df, timeframes):
    # Ensure timestamp is the index and it's in datetime format
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit="ms")
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce')
    df['Side'] = pd.to_numeric(df['Side'], errors='coerce')

    grouped = df.groupby('Instrument')

    # Store results for each timeframe
    ohlcv_results = {}
    tables = [table for table, value in TIMEFRAMES.items() if str(value) in timeframes]
    selected_timeframes = [tf for tf, tbl in OHLC_TABLE_MAPPING.items() if tbl in tables]

    try :
        for timeframe in selected_timeframes:
            if timeframe == '1D':
                continue
            resample_on = 'Timestamp'
            
            ohlcv_list = []
            for product, group in grouped:

                ohlcv = group.resample(timeframe, on=resample_on, label='right').agg({
                    'Price': ['first', 'max', 'min', 'last'],
                    'Qty': 'sum',
                })
                # Flatten column names
                ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']

                buy_volume = group[group['Side'] == 1].resample(timeframe, on=resample_on, label='right')['Qty'].sum()
                sell_volume = group[group['Side'] == -1].resample(timeframe, on=resample_on, label='right')['Qty'].sum()

                # Merge volumes into OHLCV DataFrame
                ohlcv = ohlcv.merge(buy_volume.rename('buyvolume'), left_index=True, right_index=True, how='left')
                ohlcv = ohlcv.merge(sell_volume.rename('sellvolume'), left_index=True, right_index=True, how='left')

                ohlcv[['buyvolume', 'sellvolume']] = ohlcv[['buyvolume', 'sellvolume']].fillna(0).astype(int)

                # Reset index for the resulting DataFrame
                ohlcv = ohlcv.reset_index()
                ohlcv.rename(columns={'Timestamp': 'time'}, inplace=True)
                # Add product information
                ohlcv['product'] = product
                ohlcv = ohlcv.dropna()
                ohlcv_list.append(ohlcv)
                
            # Store the result
            ohlcv_results[timeframe] = pd.concat(ohlcv_list, ignore_index=True)
    except Exception as e:
        print(e)

    return ohlcv_results

def process_chunk(df, timeframes):
    ohlcv_by_timeframe = calculate_ohlcv(df, timeframes)
    return ohlcv_by_timeframe

def process_tas_chunk_df(uuid, df, timeframes, start_datetime, end_datetime):
    start_ts = int(pd.to_datetime(start_datetime).timestamp() * 1000)
    end_ts = int(pd.to_datetime(end_datetime).timestamp() * 1000)
    print(f'above:{df}')
    df["Timestamp"] = df["Timestamp"].astype("int64")
    print(df)
    # Filter the DataFrame based on Timestamp range
    df = df[(df["Timestamp"] >= start_ts) & (df["Timestamp"] < end_ts)]
    print(f"Processing {uuid}: {len(df)} rows")
    return process_chunk(df, timeframes)
