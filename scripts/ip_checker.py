import os
import json
import ipaddress
import time
from db_utils import create_local_connection, fetch_cidr_ranges
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
    
    # Faster comparison using IPv4Network objects
    for network in cidr_networks:
        if ip_obj in network:
            return True  # IP is within this CIDR range
    return False  # IP doesn't belong to any CIDR range

def process_single_file(filename, data_folder, cidr_networks, results_folder):
    """
    Process a single file and check the IPs against the CIDR ranges.
    """
    file_start_time = time.time()  # Start time for each file
    filepath = os.path.join(data_folder, filename)
    with open(filepath, 'r') as file:
        ip_data = json.load(file)

    # Initialize results dictionary for the current file
    results = {}
    ip_check_start_time = time.time()

    # Checking each IP (batch processing could be done here for further optimization)
    for ip in ip_data.get("ip_address", []):
        is_vpn = check_ip_in_cidr(ip, cidr_networks)
        results[ip] = {"is_vpn": is_vpn}

    ip_check_end_time = time.time()
    print(f"Time to check all IPs in {filename}: {ip_check_end_time - ip_check_start_time:.2f} seconds")

    # Create a unique result file name based on the input file name
    result_filename = f"{os.path.splitext(filename)[0]}_results.json"
    result_filepath = os.path.join(results_folder, result_filename)

    # Ensure the results folder exists, then save the result file
    os.makedirs(results_folder, exist_ok=True)
    file_save_start_time = time.time()

    with open(result_filepath, 'w') as outfile:
        json.dump(results, outfile, indent=4)

    file_save_end_time = time.time()
    print(f"Time to save results for {filename}: {file_save_end_time - file_save_start_time:.2f} seconds")

    print(f"Results saved to {result_filepath}")
    file_end_time = time.time()
    print(f"Total time for {filename}: {file_end_time - file_start_time:.2f} seconds")

def process_ip_files(data_folder, cidr_networks, results_folder):
    """
    Use multiprocessing to process multiple files in parallel.
    """
    filenames = [filename for filename in os.listdir(data_folder) if filename.endswith(".json")]

    # Create a pool of workers to process files in parallel
    with Pool() as pool:
        pool.starmap(process_single_file, [(filename, data_folder, cidr_networks, results_folder) for filename in filenames])

def main():
    start_time = time.time()
    print(f"Script started at {start_time}")

    # Create a database connection
    connection = create_local_connection()
    if connection is None:
        print("Failed to connect to database.")
        return

    # Fetch and preprocess CIDR ranges from database
    cidr_fetch_start_time = time.time()
    cidr_ranges = fetch_cidr_ranges(connection)
    connection.close()
    
    # Preprocess CIDR ranges for faster lookup
    cidr_networks = preprocess_cidr_ranges(cidr_ranges)
    
    cidr_fetch_end_time = time.time()
    print(f"Time to fetch and preprocess CIDR ranges from DB: {cidr_fetch_end_time - cidr_fetch_start_time:.2f} seconds")

    # Process IP files using multiprocessing
    data_folder = os.getenv("DATA_FOLDER")
    results_folder = os.getenv("RESULTS_FOLDER")
    process_ip_files(data_folder, cidr_networks, results_folder)

    # Calculate and display execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Script execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()
