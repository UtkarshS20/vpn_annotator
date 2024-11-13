import os
import mysql.connector
from dotenv import load_dotenv
import ipaddress
import pyodbc

load_dotenv()


def create_local_connection():
    """Create connection to the local MySQL database for CIDR ranges."""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('LOCAL_DB_HOST'),
            database=os.getenv('LOCAL_DB_NAME'),
            user=os.getenv('LOCAL_DB_USER'),
            password=os.getenv('LOCAL_DB_PASSWORD')
        )
        return connection
    except mysql.connector.Error as e:
        print(f"Error connecting to local database: {e}")
        return None

# def create_hosted_connection():
#     """Create connection to the hosted SQL Server database for IP addresses."""
#     try:
#         connection = pyodbc.connect(
#             f"DRIVER={{ODBC Driver 17 for SQL Server}};"
#             f"SERVER={os.getenv('HOSTED_DB_HOST')};"
#             f"DATABASE={os.getenv('HOSTED_DB_NAME')};"
#             f"UID={os.getenv('HOSTED_DB_USER')};"
#             f"PWD={os.getenv('HOSTED_DB_PASSWORD')}"
#         )
#         return connection
#     except pyodbc.Error as e:
#         print(f"Error connecting to hosted database: {e}")
#         return None

### Fetch CIDR Ranges ###
def fetch_cidr_ranges(connection):
    """
    Fetch all CIDR ranges from the local database and preprocess them.
    
    Returns:
        list: A sorted list of ipaddress.IPv4Network objects.
    """
    cidr_query = "SELECT cidr_range FROM cidr_ranges"
    cursor = connection.cursor()
    cursor.execute(cidr_query)
    cidr_ranges = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    # Preprocess CIDR ranges into ipaddress objects and sort
    cidr_networks = [ipaddress.IPv4Network(cidr) for cidr in cidr_ranges]
    cidr_networks.sort(key=lambda net: int(net.network_address))
    return cidr_networks

### Fetch IP Addresses ###
# def fetch_ips(connection):
#     """
#     Fetch IP addresses from the hosted database.
    
#     Returns:
#         list: A list of IP addresses as strings.
#     """
#     ip_query = "SELECT ip_address FROM ip_addresses"
#     cursor = connection.cursor()
#     cursor.execute(ip_query)
#     ips = [row[0] for row in cursor.fetchall()]
#     cursor.close()
#     return ips



# import pyodbc

# # List all installed ODBC drivers
# drivers = pyodbc.drivers()

# # Print the list of drivers
# print("Installed ODBC drivers:")
# for driver in drivers:
#     print(driver)
