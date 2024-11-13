import os
import json
import ipaddress
import time
from db_utils import create_local_connection, fetch_cidr_ranges, fetch_ips, create_ebay_connection, update_is_vpn
from dotenv import load_dotenv
from multiprocessing import Pool

load_dotenv()

def preprocess_cidr_ranges(cidr_ranges):
    """
    Convert CIDR ranges to IPv4Network objects for faster lookup.
    """
    return [ipaddress.IPv4Network(cidr) for cidr in cidr_ranges]

def check_ip_in_cidr(ip, cidr_networks):
    """
    Check if the given IP is in any of the CIDR ranges using fast in-memory lookup.
    
    Args:
        ip (str): The IP address to check.
        cidr_networks (list): List of ipaddress.IPv4Network objects.
        
    Returns:
        bool: True if IP is in any CIDR range, False otherwise.
    """
    ip_obj = ipaddress.IPv4Address(ip)
    
    for network in cidr_networks:
        if ip_obj in network:
            return True
    return False

def process_ips(ip_list, cidr_networks):
    """
    Process a list of IPs and check each one against the CIDR ranges, 
    updating the is_vpn field in the eBay DB.
    """
    ip_list = list(set(ip_list))
    ebay_connection = create_ebay_connection()  
    if not ebay_connection:
        print("Failed to connect to eBay database.")
        return
    
    ip_check_start_time = time.time()

    for ip in ip_list:
        is_vpn = check_ip_in_cidr(ip, cidr_networks)
        update_is_vpn(ebay_connection, ip, is_vpn)

    ip_check_end_time = time.time()
    print(f"Time to check all IPs and update eBay DB: {ip_check_end_time - ip_check_start_time:.2f} seconds")

    ebay_connection.close()

def process_ips_in_parallel(ip_list, cidr_networks):
    """
    Use multiprocessing to process multiple IPs in parallel and update the eBay DB.
    """
    chunk_size = 100
    ip_chunks = [ip_list[i:i + chunk_size] for i in range(0, len(ip_list), chunk_size)]

    with Pool() as pool:
        pool.starmap(process_ips, [(chunk, cidr_networks) for chunk in ip_chunks])


def main():
    start_time = time.time()
    print(f"Script started at {start_time}")

    local_connection = create_local_connection()
    if local_connection is None:
        print("Failed to connect to local database.")
        return

    cidr_fetch_start_time = time.time()
    cidr_ranges = fetch_cidr_ranges(local_connection)
    local_connection.close()
    
    cidr_networks = preprocess_cidr_ranges(cidr_ranges)
    
    cidr_fetch_end_time = time.time()
    print(f"Time to fetch and preprocess CIDR ranges from DB: {cidr_fetch_end_time - cidr_fetch_start_time:.2f} seconds")

    ebay_connection = create_ebay_connection()
    if ebay_connection is None:
        print("Failed to connect to eBay database.")
        return

    ip_list = fetch_ips(ebay_connection)
    print(f"Fetched {len(ip_list)} IPs.")

    process_ips_in_parallel(ip_list, cidr_networks)

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Script execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()