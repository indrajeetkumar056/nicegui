import pandas as pd
import psycopg2
from configs import *
        
def bulk_insert_into_db(db_name, table_name, df):
    """
    Perform bulk insert into the database.
    """
    try:
        # Assuming you are using psycopg2 for PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=db_name,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        columns = ', '.join([f'"{col}"' for col in df.columns])
        placeholders = ', '.join(['%s'] * len(df.columns))

        insert_query = f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders});
        """
        buffer = df.values.tolist()
        # Execute bulk insert
        cursor.executemany(insert_query, buffer)

        # Commit the transaction
        conn.commit()
        print(f"Successfully inserted {df.shape[0]} records in bulk to {table_name}.")

    except Exception as e:
        print(f"Error during bulk insert: {e}")
    finally:
        cursor.close()
        conn.close()