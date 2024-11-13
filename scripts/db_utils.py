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

def create_ebay_connection():
    """Create connection to the hosted SQL Server database for IP addresses."""
    try:
        connection = pyodbc.connect(
            f"DRIVER={{MySQL ODBC 9.1 Unicode Driver}};"
            f"SERVER={os.getenv('EBAY_DB_HOST')};"
            f"DATABASE={os.getenv('EBAY_DB_NAME')};"
            f"UID={os.getenv('EBAY_DB_USER')};"
            f"PWD={os.getenv('EBAY_DB_PASSWORD')}"
        )
        return connection
    except pyodbc.Error as e:
        print(f"Error connecting to hosted database: {e}")
        return None

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

def fetch_ips(connection):
    """
    Fetch IP addresses from the hosted database.
    
    Returns:
        list: A list of IP addresses as strings.
    """
    ip_query = "SELECT CLNT_RMT_IP FROM tbl_iptrace_user_activity"
    cursor = connection.cursor()
    cursor.execute(ip_query)
    ips = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return ips

def update_is_vpn(ebay_connection, ip, is_vpn):
    """
    Update the is_vpn field for a given IP in the eBay database.
    
    Args:
        ebay_connection (connection object): The connection object to the eBay database.
        ip (str): The IP address.
        is_vpn (bool): True if the IP is a VPN, False otherwise.
    
    Returns:
        None
    """
    try:
        cursor = ebay_connection.cursor()
        update_query = """
            UPDATE tbl_iptrace_user_activity
            SET is_vpn = ?
            WHERE CLNT_RMT_IP = ?;
        """
        cursor.execute(update_query, (is_vpn, ip))
        ebay_connection.commit()
        cursor.close()
    except Exception as e:
        print(f"Error updating is_vpn for IP {ip}: {e}")
        ebay_connection.rollback()
