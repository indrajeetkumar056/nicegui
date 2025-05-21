from nicegui import ui
import pandas as pd

class TableManager:
    """Manages multiple UI tables (fetched & processed) with pagination."""

    fetched_table = None  
    processed_tables = {}  # ✅ Store separate processed tables
    df_store = {}  
    current_page = {}  
    rows_per_page = 10  
    total_pages = {}  
    page_labels = {}  

    @classmethod
    def create_table(cls, table_type="fetched", table_name=None):
        """Creates a table instance for fetched or processed data."""
        table_instance = ui.table(columns=[], rows=[]).classes('w-full mt-4')

        if table_type == "processed" and table_name:
            cls.processed_tables[table_name] = table_instance
            cls.current_page[table_name] = 1
            cls.total_pages[table_name] = 1
            cls.df_store[table_name] = pd.DataFrame()
        else:
            cls.fetched_table = table_instance
            cls.current_page[table_type] = 1
            cls.total_pages[table_type] = 1
            cls.df_store[table_type] = pd.DataFrame()

        with ui.row().classes('w-full justify-between mt-2'):
            prev_btn = ui.button(icon="chevron_left", on_click=lambda: cls.change_page(table_type, table_name, -1)).classes('reset-button')
            cls.page_labels[table_type if table_name is None else table_name] = ui.label("Page 1 of 1").classes('text-md')
            next_btn = ui.button(icon="chevron_right", on_click=lambda: cls.change_page(table_type, table_name, 1)).classes('reset-button')

        return table_instance

    @classmethod
    def update_table(cls, df, table_type="fetched", table_name=None):
        """Updates the respective table and resets pagination."""
        if table_type == "processed" and table_name:
            cls.df_store[table_name] = df
            cls.total_pages[table_name] = max(1, -(-len(df) // cls.rows_per_page))
            cls.current_page[table_name] = 1
            cls.refresh_table(table_type, table_name)
        else:
            cls.df_store[table_type] = df
            cls.total_pages[table_type] = max(1, -(-len(df) // cls.rows_per_page))
            cls.current_page[table_type] = 1
            cls.refresh_table(table_type)

    @classmethod
    def refresh_table(cls, table_type, table_name=None):
        """Refreshes the fetched or processed table with pagination."""
        table_instance = cls.fetched_table if table_name is None else cls.processed_tables.get(table_name)
        df = cls.df_store[table_name if table_name else table_type]

        if table_instance and not df.empty:
            start_idx = (cls.current_page[table_name if table_name else table_type] - 1) * cls.rows_per_page
            end_idx = start_idx + cls.rows_per_page

            table_instance.columns = [{'name': col, 'label': col, 'field': col, 'sortable': True} for col in df.columns]
            table_instance.rows = df.iloc[start_idx:end_idx].to_dict(orient='records')

            table_instance.update()

            # ✅ Update page label dynamically
            page_label_key = table_name if table_name else table_type
            if page_label_key in cls.page_labels:
                cls.page_labels[page_label_key].set_text(f"Page {cls.current_page[page_label_key]} of {cls.total_pages[page_label_key]}")

    @classmethod
    def change_page(cls, table_type, table_name, delta):
        """Handles pagination for each table separately."""
        key = table_name if table_name else table_type
        new_page = cls.current_page[key] + delta

        if 1 <= new_page <= cls.total_pages[key]:
            cls.current_page[key] = new_page
            cls.refresh_table(table_type, table_name)

    @classmethod
    def clear_table(cls):
        """Clears all rows and columns from the table."""
        if cls.fetched_table:
            cls.fetched_table.columns = []
            cls.fetched_table.rows = []
            cls.df_store["fetched"] = pd.DataFrame()  # Reset stored data
            cls.current_page["fetched"] = 1
            cls.total_pages["fetched"] = 1
            cls.page_labels["fetched"].set_text("Page 1 of 1")  # Reset label
            cls.fetched_table.update
        