from nicegui import ui
import pandas as pd
from functools import partial
from utils.datastore import Datastore
from app.niceGUI.utils.table_manager import TableManager
def show_processed_data():
    """
    Displays the processed data from Datastore.processed_dict in the UI with independent pagination.
    """
    if not Datastore.processed_dict:
        #ui.notify("No processed data available!", type="warning")
        return

    try:
        if Datastore.process_tables_container:
            Datastore.process_tables_container.clear()

        with Datastore.process_tables_container:
            for sink_name, data in Datastore.processed_dict.items():
                # Case 1: Single DataFrame sink (e.g., {"csv": df})
                if isinstance(data, pd.DataFrame):
                    df = data
                    if df is None or df.empty:
                        #ui.notify(f"{sink_name} is empty!", type="warning")
                        continue  

                    if 'time' in df.columns:
                        df['time'] = pd.to_datetime(df['time'], errors='coerce')
                        df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

                    with ui.expansion(f"Sink: {sink_name}", icon="database").classes("w-full mt-4"):
                        with ui.card().classes('w-full p-4'):
                            ui.label(f"Processed Data: {sink_name}").classes('text-lg font-bold')
                            table = TableManager.create_table(table_type="processed", table_name=sink_name)
                            TableManager.update_table(df, table_type="processed", table_name=sink_name)

                # Case 2: Multi-timeframe sink (e.g., {"dbohlc": {"1T": df, "5T": df}})
                elif isinstance(data, dict):
                    with ui.expansion(f"Sink: {sink_name}", icon="database").classes("w-full mt-4"):
                        for timeframe, df in data.items():
                            if df is None or df.empty:
                                #ui.notify(f"{sink_name} - {timeframe} is empty!", type="warning")
                                continue  

                            if 'time' in df.columns:
                                df['time'] = pd.to_datetime(df['time'], errors='coerce')
                                df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

                            with ui.expansion(f"Timeframe: {timeframe}").classes("w-full"):
                                with ui.card().classes('w-full p-4'):
                                    ui.label(f"Processed Data: {sink_name} - {timeframe}").classes('text-lg font-bold')
                                    table = TableManager.create_table(table_type="processed", table_name=f"{sink_name}_{timeframe}")
                                    TableManager.update_table(df, table_type="processed", table_name=f"{sink_name}_{timeframe}")

        #ui.notify("Processed data displayed successfully!", type="success")

    except Exception as e:
        print(e)
        #ui.notify(f"Error displaying processed data: {str(e)}", type="error")
