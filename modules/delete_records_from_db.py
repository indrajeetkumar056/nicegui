import pandas as pd
import psycopg2
from configs import *

def delete_records_from_db(db_name, table_name, products, min_timestamp, max_timestamp):
    """
    Delete records from raw_ticks_prod table where timestamp is between min_timestamp and max_timestamp
    for the given products.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=db_name,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        
        # SQL query to delete records between the given timestamps for each product
        for product in products:
            print(product)
            if pd.isna(product):
                continue
            delete_query = f"""
            DELETE FROM {table_name}
            WHERE "Instrument" = %s AND "Timestamp" BETWEEN %s AND %s;
            """
            cursor.execute(delete_query, (product, min_timestamp, max_timestamp))

        conn.commit()
        cursor.close()
        conn.close()
        print("Deleted old records from DB.")

    except Exception as e:
        print(f"Error deleting records from DB: {e}")