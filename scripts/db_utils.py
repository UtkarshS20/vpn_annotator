# import os
# import mysql.connector
# from dotenv import load_dotenv

# load_dotenv()

# def create_connection():
#     try:
#         connection = mysql.connector.connect(
#             host=os.getenv('DB_HOST'),
#             database=os.getenv('DB_NAME'),
#             user=os.getenv('DB_USER'),
#             password=os.getenv('DB_PASSWORD')
#         )
#         return connection
#     except mysql.connector.Error as e:
#         print(f"Error: {e}")
#         return None

# def fetch_cidr_ranges(connection):
#     """
#     Fetch all CIDR ranges from the cidr_ranges table.
    
#     Returns:
#         list of tuples: Each tuple contains a CIDR range.
#     """
#     cidr_query = "SELECT cidr_range FROM cidr_ranges"
#     cursor = connection.cursor()
#     cursor.execute(cidr_query)
#     cidr_ranges = [row[0] for row in cursor.fetchall()]
#     cursor.close()
#     return cidr_ranges

# db_utils.py
import ipaddress
import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        return connection
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return None

def fetch_cidr_ranges(connection):
    """
    Fetch all CIDR ranges from the cidr_ranges table and preprocess them for faster lookup.
    
    Returns:
        list: A sorted list of ipaddress.IPv4Network objects.
    """
    cidr_query = "SELECT cidr_range FROM cidr_ranges"
    cursor = connection.cursor()
    cursor.execute(cidr_query)
    cidr_ranges = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    # Preprocess CIDR ranges into ipaddress objects
    cidr_networks = [ipaddress.IPv4Network(cidr) for cidr in cidr_ranges]
    
    # Sort the CIDR ranges (this will allow efficient searching)
    cidr_networks.sort(key=lambda net: int(net.network_address))
    return cidr_networks
