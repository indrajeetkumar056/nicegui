import pandas as pd
from configs import *

def calculate_vap(df, timeframes):
    # Ensure timestamp is the index and it's in datetime format
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit="ms")
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce')
    df['Side'] = pd.to_numeric(df['Side'], errors='coerce')

    
    # Store results for each timeframe
    vap_results = {}
    tables = [table for table, value in VAP_TIMEFRAMES.items() if str(value) in timeframes]
    selected_timeframes = [tf for tf, tbl in VAP_TABLE_MAPPING.items() if tbl in tables]

    try:
        for timeframe in selected_timeframes:
            if timeframe == "1D":
                continue  # Skip daily timeframe

            vap_list = []

            df_resampled = df.groupby([
                pd.Grouper(key="Timestamp", freq=timeframe, label="right"), "Instrument", "Price"
            ]).agg(
                buyVolume=pd.NamedAgg(column="Qty", aggfunc=lambda x: x[df.loc[x.index, "Side"] == 1].sum()),
                sellVolume=pd.NamedAgg(column="Qty", aggfunc=lambda x: x[df.loc[x.index, "Side"] == -1].sum())
            ).fillna(0).reset_index()

            for (time, product), group in df_resampled.groupby(["Timestamp", "Instrument"]):
                vap_entries = [
                    f"{row['Price']}:{int(row['buyVolume'])}-{int(row['sellVolume'])}"
                    for _, row in group.iterrows()
                ]

                if vap_entries:
                    vap_list.append({
                        "time": time,
                        "product": product,
                        "vap": ";".join(vap_entries)
                    })

            vap_df = pd.DataFrame(vap_list)
            vap_results[timeframe] = vap_df
    except Exception as e:
        print(e)

    return vap_results

def process_chunk(df, timeframes):
    ohlcv_by_timeframe = calculate_vap(df, timeframes)
    return ohlcv_by_timeframe

def process_vap_chunk_async(uuid, df, timeframes, start_datetime, end_datetime):
    start_ts = int(pd.to_datetime(start_datetime).timestamp() * 1000)
    end_ts = int(pd.to_datetime(end_datetime).timestamp() * 1000)
    df["Timestamp"] = df["Timestamp"].astype(int)

    # Filter the DataFrame based on Timestamp range
    df = df[(df["Timestamp"] >= start_ts) & (df["Timestamp"] < end_ts)]
    print(f"Processing {uuid}: {len(df)} rows")
    return process_chunk(df, timeframes)

