import pyodbc
import mysql.connector
import dask.dataframe as dd
import pandas as pd
from db_utils import create_ebay_connection
import os 
from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
conn = create_ebay_connection()

query = "SELECT * FROM tbl_iptrace_user_activity"


def fetch_data_in_chunks(conn, query, chunk_size=10000):
    """
    Fetch data from the database in chunks and yield each chunk as a Pandas DataFrame.
    """
    cursor = conn.cursor()
    cursor.execute(query)

    while True:
        # Fetch the next chunk of rows
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break
        # Convert the chunk to a Pandas DataFrame
        columns = [column[0] for column in cursor.description]  # Get column names
        if len(columns) != len(rows[0]):  
            print(f"Warning: Column count mismatch: expected {len(columns)}, got {len(rows[0])}")
        yield pd.DataFrame(rows, columns=columns)

# Now, use Dask to create a DataFrame from the chunks
dask_df = dd.from_delayed(
    [dd.from_pandas(chunk, npartitions=1) for chunk in fetch_data_in_chunks(conn, query)]
)

# Check the Dask DataFrame
print(dask_df.head())

# print(conn)