from nicegui import ui, app
import pandas as pd

from configs import *
from utils.helper import get_available_sources, get_available_sinks
from app.niceGUI.utils.update_products_on_toggle import toggle_select_all, handle_file_upload, on_product_type_change
from app.niceGUI.utils.table_manager import TableManager
from utils.datastore import Datastore
from app.niceGUI.utils.buttons_functions import fetch_button, sink_button, process_button,reset_fields,check_inputs_and_toggle_reset,download_df,run_code

# Set the CSS for the page 
app.add_static_files('/static', 'app/niceGUI/static')

def manual_card_manager():
    manual_card = None
    ui_context = {}

    def open_manual_card():
        nonlocal manual_card

        available_sources = get_available_sources()
        available_products = list(PRODUCTS_CRITERIA.keys())
        available_sinks = get_available_sinks()

        if manual_card is None:
          
                # Manual Card (Left Side)
               
                    manual_card = ui.card().classes('nicegui-card')

                    ui.add_head_html('<link rel="stylesheet" type="text/css" href="static/styles.css">')

                    with manual_card:
                        with ui.row().classes("w-full"):
                            ui.label('Manual Backfill Options').classes('text-2xl font-bold text-center')
                            
                            ui.space()  # Pushes the button to the right
                            
                            reset_btn = ui.button(icon="refresh", on_click=lambda: reset_fields(
                                source_selector, product_type, product_selector, start_datetime, end_datetime, reset_btn,ui_context
                            )).classes("reset-button")
                            reset_btn.set_visibility(False)  # Hide initially
                        
                        # SOURCE & PRODUCTS SELECTOR
                        with ui.row().classes('w-full border-0 input-container'):
                            source_selector = ui.select(available_sources, label="Source directory", with_input=True, clearable=True).classes('nicegui-input')
                            product_type = ui.select(available_products + ['CSV/Parquet'], label='Products Type', with_input=True, clearable=True).classes('nicegui-input')
                            file_upload = ui.upload(on_upload=lambda e: handle_file_upload(e, product_selector), label='Upload CSV or Parquet')
                            file_upload.set_visibility(False)
                            product_selector = ui.select([], multiple=True, label='Select Products', with_input=True, clearable=True).classes("nicegui-input input-box w-64 max-h-20 overflow-hidden text-ellipsis")
                            start_datetime = ui.input(label="Start Date & Time").props('type=datetime-local').classes('nicegui-input')
                            end_datetime = ui.input(label="End Date & Time").props('type=datetime-local').classes('nicegui-input')
                        source_selector.on_value_change(lambda e: check_inputs_and_toggle_reset(
                          source_selector, product_type, product_selector, start_datetime, end_datetime, reset_btn
                        ))

                        # Combine with existing logic for product_type
                        product_type.on_value_change(lambda e: (
                            on_product_type_change(e, file_upload, product_selector),
                            check_inputs_and_toggle_reset(source_selector, product_type, product_selector, start_datetime, end_datetime, reset_btn)
                        ))

                        # Combine with existing logic for product_selector
                        product_selector.on_value_change(lambda e: (
                            toggle_select_all(e, product_selector),
                            check_inputs_and_toggle_reset(source_selector, product_type, product_selector, start_datetime, end_datetime, reset_btn)
                        ))

                        start_datetime.on_value_change(lambda e: check_inputs_and_toggle_reset(
                            source_selector, product_type, product_selector, start_datetime, end_datetime, reset_btn
                        ))
                        end_datetime.on_value_change(lambda e: check_inputs_and_toggle_reset(
                            source_selector, product_type, product_selector, start_datetime, end_datetime, reset_btn
                        ))

                            

                        # FETCH DATA BUTTON & FETCHED DATA TABLE
                        with ui.row():
                            fetch_ui_button = ui.button("FETCH DATA", on_click=lambda: fetch_button(source_selector, product_selector, start_datetime, end_datetime, ui_context,fetch_ui_button,download_button)).classes('bg-red-500 text-white').classes("nicegui-button")
                            
                            
                        fetch_expansion = ui.expansion("Fetched Data", icon="table").classes('w-full mt-4')
                        with fetch_expansion:
                            download_button = ui.button(icon="download", on_click=lambda: download_df(ui_context))
                            
                            TableManager.create_table("fetched")   # Table manager is used to create a table for fetched data

                        # PROCESS CONTAINER (Initially Hidden)
                        with ui.column().classes("w-full") as process_container:
                            process_container.set_visibility(False)  # Hide by default

                            with ui.row().classes('w-full border-0 datetime-container'):
                                sink_selector = ui.select(available_sinks, label="Select sink", with_input=True, clearable=True, multiple=True).classes('nicegui-input')
                                ui.space()
                                arguments = ui.input("KWARGS").classes('nicegui-input')
                                editor = ui.codemirror('print("Edit me!")', language='Python').classes('h-32')
                                ui.select(editor.supported_languages, label='Language', clearable=True) \
                                    .classes('w-32').bind_value(editor, 'language')
                                ui.select(editor.supported_themes, label='Theme') \
                                    .classes('w-32').bind_value(editor, 'theme')
                            ui.button("Run Code", on_click=run_code(editor,ui_context)).classes("nicegui-button")
                            # PROCESS DATA BUTTON
                            process_ui_button = ui.button("Process Data", on_click=lambda: process_button(
                                sink_selector, product_selector, start_datetime, end_datetime, process_ui_button,kwargs=arguments,ui_context=ui_context
                            )).classes("nicegui-button")
                           
                            # PROCESS TABLE CONTAINER
                            process_tables_container = ui.row().classes("w-full flex")
                            Datastore.set_process_table_container(process_tables_container)

                        # SINK DATA BUTTON
                        with ui.column().classes("w-full") as sink_container:
                            sink_container.set_visibility(False)  # Initially hidden
                            sink_ui_button = ui.button("Sink Processed Data", on_click=lambda: sink_button(sink_selector,sink_ui_button, kwargs=arguments)).classes("nicegui-button")

                        # Store process container & sink container in UI context
                        ui_context["process-container"] = process_container
                        ui_context["sink_container"] = sink_container

                # Logger UI (Right Side)
              

    return open_manual_card, ui_context

# Create an instance for manual card 
open_manual_card, ui_context = manual_card_manager()
