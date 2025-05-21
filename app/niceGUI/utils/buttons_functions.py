import pandas as pd
from datetime import datetime
from nicegui import ui
import asyncio
import os
from utils.datastore import Datastore
from utils.class_imports import fetch_data_from_source, process_fetched_data, dump_data_to_sink
from app.niceGUI.utils.table_manager import TableManager
from app.niceGUI.utils.logger_ui import LOGGER_REGISTRY
from app.niceGUI.utils.show_processed_data import *
async def fetch_button(source_selector, product_selector, start_datetime, end_datetime, ui_context, fetch_ui_button, download_button):
    """Handles UI updates and fetches data asynchronously."""
    fetch_ui_button.disable()
   
    ui.update()
    
    try:
        current_time = datetime.now()
        end_time_value = pd.to_datetime(end_datetime.value)
        if end_time_value > current_time:
            ui.notify("❌ End time cannot be in the future!", type="error")
            fetch_ui_button.enable()
            ui.update()
            return  # Exit the function early

        # Call the async function inside `asyncio.to_thread`
        df, message, status = await fetch_data_from_source(
            source_selector.value, product_selector.value, start_datetime.value, end_datetime.value
        )

        if status:
            ui_context["fetched_df"] = df  # Store fetched DataFrame for downloading
            process_container = ui_context.get("process-container")
            if process_container:
                process_container.set_visibility(True)
                ui.update()
            else:
                print("UI Issue.")

           
    except asyncio.TimeoutError:
        ui.notify("⏳ Fetching took too long! Please try again.", type="warning")
        print("⏳ Fetching timed out after 60 seconds.")

    except Exception as e:
        ui.notify(f"❌ An error occurred: {str(e)}", type="error")
        print(f"❌ Error: {e}")

    finally:
        fetch_ui_button.enable()
        ui.update()

def download_df(ui_context):
    """Converts the fetched DataFrame to Parquet and provides a download link."""
    df = ui_context.get("fetched_df")
    if df is None or df.empty:
        ui.notify("⚠ No data available for download!", type="warning")
        return

    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    parquet_path = os.path.join(downloads_folder, "fetched_data.parquet")
    df.to_parquet(parquet_path)
    ui.download(parquet_path, filename="fetched_data.parquet")

async def process_button(sink_selector, product_selector, start_datetime, end_datetime, process_ui_button, kwargs, ui_context):
    """Processes based on specified sink and shows the Sink Data button only if successful."""
    
    process_ui_button.disable()  # Disable button during processing
    ui.update()
    
    logger_ui = LOGGER_REGISTRY.get("manual_logger")  
    try:
        process_results = await process_fetched_data(
            sink_selector.value, product_selector.value, start_datetime.value, end_datetime.value,
            callback_function=None, kwargs=kwargs.value if kwargs.value else None
        )
        
        # ✅ Check if all sinks processed successfully
        all_success = all(status for status, message in process_results.values())
        if all_success:
            logger_ui.log_message(f"✅ Processing completed for all sinks.")
        
        sink_container = ui_context.get("sink_container")
        if sink_container:
            sink_container.set_visibility(all_success)  # Show only if all processes are successful
            ui.update()
        else:
            ui.notify("❌ Some processes failed. Check logs for details.", type="error")
    
    except Exception as e:
        ui.notify(f"❌ An error occurred: {str(e)}", type="error")
        print(f"❌ Error: {e}")

    finally:
        process_ui_button.enable()  # Re-enable button after processing
        ui.update()

      
async def sink_button(sink_selector, sink_ui_button,kwargs ):
    """Dumps the processed data to the selected sink."""
    logger_ui = LOGGER_REGISTRY.get("manual_logger")

    # Disable the button and show a loading spinner
    sink_ui_button.disable()
    sink_ui_button.props('loading')  # Shows a loading animation on the button

    try:
        sink_results = await dump_data_to_sink(
            sink_selector.value,
            callback_function=None,
            kwargs=kwargs.value if kwargs.value else None
        )

        all_success = all(status for status, message in sink_results.values())

        if all_success:
            ui.notify("✅ Data Sunked Successfully", type='positive')
            logger_ui.log_message(f"✅ All results after sinking: {sink_results}")
        else:
            ui.notify("❌ Some sinks failed. Check logs for details.", type='error')

    except Exception as e:
        ui.notify(f"❌ Error during sinking: {str(e)}", type='negative')
        logger_ui.log_message(f"❌ Error during sinking: {str(e)}")

    finally:
        sink_ui_button.enable()
        sink_ui_button.props(remove='loading')  # Remove loading animation


def reset_fields(source_selector, product_type, product_selector, start_datetime, end_datetime, reset_btn,ui_context):
    """Clears all input fields and hides the reset button if fields are empty."""
    source_selector.set_value(None)
    product_type.set_value(None)
    product_selector.set_value([])
    start_datetime.set_value(None)
    end_datetime.set_value(None)
    reset_btn.set_visibility(False)  # Hide Reset button
    
    
    if Datastore.process_tables_container:
        Datastore.process_tables_container.clear()
    
    
    # ✅ Hide process container and sink container
    ui_context["process-container"].set_visibility(False)
    ui_context["sink_container"].set_visibility(False)

    ui.notify("🔄 Fields reset successfully!", type="info")
    ui.update()


def check_inputs_and_toggle_reset(source_selector, product_type, product_selector, start_datetime, end_datetime, reset_btn):
    """Show reset button only if any field has input."""
    if any([
        source_selector.value, 
        product_type.value, 
        product_selector.value, 
        start_datetime.value, 
        end_datetime.value
    ]):
        reset_btn.set_visibility(True)  # Show Reset button if any field has value
    else:
        reset_btn.set_visibility(False)  # Hide if all fields are empty
    TableManager.clear_table()
    
    ui.update()


def run_code(editor,ui_context):
        """Fetches the DataFrame, applies transformations, and updates the UI."""
        df = ui_context.get("fetched_df")

        if df is None or df.empty:
            ui.notify("⚠ No fetched data available!", type="warning")
            return
        
        try:
            code_to_execute = editor.value  # Get the code from the editor
            local_scope = {"df": df.copy()}  # Provide df in local scope

            exec(code_to_execute, {}, local_scope)  # Run the code safely

            # Get the modified DataFrame
            processed_df = local_scope.get("df", None)
            if processed_df is None or processed_df.empty:
                ui.notify("⚠ No data processed!", type="warning")
                return
            
            # Store processed DataFrame
            Datastore.processed_dict["Custom Processed Data"] = processed_df
            
            # Update Process Table UI
            show_processed_data()

            ui.notify("✅ Data processed successfully!", type="success")

        except Exception as e:
            ui.notify(f"❌ Error: {str(e)}", type="error")