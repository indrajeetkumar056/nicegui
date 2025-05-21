from configs import *
from utils.helper import fetch_products_from_instrument_url, get_available_sinks, get_available_sources
from modes.automatic_backfill_main import automatic_backfill_function
# from modes.manual_backfill_main import manual_backfill_function
from sources import SourceBase
from sinks import SinkBase
import asyncio
def run_terminal():
    mode = input("Enter mode (manual/automatic): ").strip().lower()
    if mode == 'automatic':
        print(f"Automatic mode selected. Processing will start directly.")
        # automatic_backfill_function()
    else:
        available_sources = get_available_sources()
        available_products = list(PRODUCTS_CRITERIA.keys())
        available_sinks = get_available_sinks()

        if not available_sources:
            print(f"No sources available: {mode}")
            exit()

        # Source selection
        print("\nAvailable sources:")
        for idx, source in enumerate(available_sources, 1):
            print(f"{idx}. {source}")
        try: 
            source_choice = int(input("\nSelect source by number (only one):").strip())
            if 1 <= source_choice <= len(available_sources):
                selected_source = available_sources[source_choice - 1]
                print(f"Selected source: {selected_source}")
            else:
                print("Invalid choice. Exiting.")
                return
        except ValueError:
            print("Invalid input. Please enter a number.")
            return

        # Get start and end time
        try:
            start_datetime = input("\nEnter start date (YYYY-MM-DD HH:MM:SS): ").strip()
            
            end_datetime = input("\nEnter end date (YYYY-MM-DD HH:MM:SS): ").strip()
             
        except ValueError:
            print("Invalid date/time format. Please follow YYYY-MM-DD and HH:MM:SS.")
            return

          # Product selection
    print("\nAvailable product types:")
    for idx, product in enumerate(available_products, 1):
        print(f"{idx}. {product}")
   
    product_type_choice = input("\nSelect product type by number: ").strip()
    if product_type_choice.isdigit() and 1 <= int(product_type_choice) <= len(available_products):
        selected_product_type = available_products[int(product_type_choice) - 1]
        products_url = PRODUCTS_CRITERIA[selected_product_type]
    else:
        print("Invalid product type selection. Exiting.")
        return
    selected_products = input("\nEnter products (comma-separated): ").strip().split(',')
    selected_products = [p.strip() for p in selected_products if p.strip()]
   
    if not selected_products:
        print("No products entered. Exiting.")
        return
   
    print(f"Selected products: {selected_products}")
    print(f"Fetched products: {selected_products}")
    # Fetch Data step
    print("Fetching data...")
    SourceBase.fetch_data_from_source(selected_source, selected_products, start_datetime, end_datetime)
    print("Data fetched successfully.")
 
    # Sink selection
    print("\nAvailable sinks:")
    for idx, sink in enumerate(available_sinks, 1):
        print(f"{idx}. {sink}")

    try:
        sink_choices = input("\nSelect sink(s) by number (comma-separated): ").strip()
        selected_sinks = [available_sinks[int(choice) - 1] for choice in sink_choices.split(",") if choice.strip().isdigit()]

        if not selected_sinks:
            print("No valid sinks selected. Exiting.")
            return

        print(f"Selected sinks: {', '.join(selected_sinks)}")

    except (ValueError, IndexError):
        print("Invalid input. Please enter numbers separated by commas.")
        return
    
   
    # Additional arguments for processing
    arguments = input("Enter additional processing arguments (leave empty if none): ").strip()
    arguments = arguments if arguments else None
 
    # Process Data step
    process_choice = input("Do you want to process the data? (yes/no): ").strip().lower()
    if process_choice == "yes":
        print("Processing data...")
        SinkBase.process_fetched_data(selected_sinks, callback_function=None, kwargs=arguments)
        print("Data processed successfully.")
    else:
        print("Skipping data processing.")

    # Ask user before sinking data
    sink_choice = input("Do you want to sink the processed data? (yes/no): ").strip().lower()
    if sink_choice == "yes":
        print("Sinking processed data...")
        SinkBase.dump_data_to_sink(selected_sinks)
        print("Processed data successfully sunk.")
    else:
        print("Skipping data sinking.")
 
