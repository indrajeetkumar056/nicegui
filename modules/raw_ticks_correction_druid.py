import json
import requests
import urllib3
import http.server
import socket
import socketserver
import threading
import time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from configs import *

# Step 1: Reindex Druid data excluding specified products
def reindex_druid_data(start_datetime, end_datetime, products=None, dimensions=None):
    if dimensions is None:
        dimensions = ["instrument", "price", "qty"]  # Default dimensions
    if not products:
        print("No products to exclude, skipping reindexing.")
        return
    exclusion_filter = {
        "type": "not",
        "field": {
            "type": "in",
            "dimension": "instrument",
            "values": products
        }
    }
    input_source = {
        "type": "druid",
        "dataSource": DRUID_DATASOURCE,
        "interval": f"{start_datetime}/{end_datetime}",
        "filter": exclusion_filter
    }
    
    reindex_spec = {
        "type": "index",
        "spec": {
            "dataSchema": {
                "dataSource": DRUID_DATASOURCE,
                "timestampSpec": {"column": "__time", "format": "auto"},
                "dimensionsSpec": {"dimensions": dimensions},
            },
            "ioConfig": {
                "type": "index",
                "inputSource": input_source,
                "inputFormat": {"type": "json"}
            },
            "tuningConfig": {"type": "index", "maxRowsInMemory": 10000}
        }
    }
    try:
        response = requests.post(
            f"{DRUID_OVERLORD_URL}/druid/indexer/v1/task",
            headers={"Content-Type": "application/json"},
            data=json.dumps(reindex_spec),
            verify=False  
        )
        response.raise_for_status() 
        response.json().get("task")
        print(f"Reindexing initiated.")

    except requests.exceptions.RequestException as e:
        print(f"Failed to reindex: {str(e)}")

def fetch_task_logs(task_id):
    url = f"{DRUID_OVERLORD_URL}/druid/indexer/v1/task/{task_id}/log"
    response = requests.get(url, verify=False)  # Disable SSL verification if using self-signed certificates

    if response.status_code == 200:
        print("Task logs:")
        print(response.text)  # Print logs to the console
    else:
        print(f"Failed to fetch task logs. Status code: {response.status_code}, Response: {response.text}")

# Step 2: Starting HTTP server
def start_http_server(directory=".", port=8000):
    handler = http.server.SimpleHTTPRequestHandler
    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True
    # Check if the port is available
    def is_port_in_use(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex((DRUID_SERVER_URL, port)) == 0

    # Find a free port
    original_port = port
    while is_port_in_use(port):
        print(f"Port {port} is already in use. Trying next port...")
        port += 1  # Increment the port number

    # Log the port being used
    if port != original_port:
        print(f"Using port {port} for the HTTP server.")

    # Start the HTTP server
    httpd = ReusableTCPServer(("", port), handler)
    httpd.directory = directory

    # Run the server in a separate thread
    def serve_forever():
        print(f"HTTP server is running at http://{DRUID_SERVER_URL}:{port}")
        httpd.serve_forever()

    thread = threading.Thread(target=serve_forever, daemon=True)
    thread.start()

    return httpd, f"http://{DRUID_SERVER_URL}:{port}"


# Step 3: Backfill Druid data using the HTTP file
def backfill_druid_with_http(file_url):
    csv_ingestion_spec = {
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": DRUID_DATASOURCE,
                "timestampSpec": {"column": "timestamp", "format": "millis"},
                "dimensionsSpec": {"dimensions": [
                    "instrument",
                    {
                    "type": "double",
                    "name": "price"
                    },
                    "source",
                    {
                    "type": "long",
                    "name": "side"
                    },
                    {""
                    "type": "long", 
                    "name": "qty"
                    }
                    ]
                },
                "granularitySpec": {
                    "queryGranularity": "none",
                    "rollup": False,
                    "segmentGranularity": "day"
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "http",  
                    "uris": [file_url]  
                },
                "inputFormat": {
                    "type": "csv",
                    "findColumnsFromHeader": True,
                    "listDelimiter": "\u0001",
                    "skipHeaderRows": 0
                },
                "appendToExisting": True
            },
            "tuningConfig": {
                "type": "index_parallel",
                "partitionsSpec": {
                    "type": "dynamic"
                }
            }
        }
    }

    response = requests.post(
        f"{DRUID_OVERLORD_URL}/druid/indexer/v1/task",
        headers={"Content-Type": "application/json"},
        data=json.dumps(csv_ingestion_spec),
        verify=False  # Disable SSL verification
    )
    if response.status_code == 200:
        task_id = response.json().get("task")
        print(f"CSV ingestion via HTTP initiated. Task ID: {task_id}")
        return task_id
    else:
        print(f"Failed to start CSV ingestion: {response.text}")
        return None


# Step 4: Monitor Druid task status
def monitor_druid_task(task_id):
    print("Monitoring Druid task status...")
    while True:
        response = requests.get(
            f"{DRUID_OVERLORD_URL}/druid/indexer/v1/task/{task_id}/status",
            verify=False  # Disable SSL verification
        )
        if response.status_code == 200:
            status = response.json().get("status", {}).get("status")
            print(f"Task status: {status}")
            if status in ["SUCCESS", "FAILED"]:
                return status
        else:
            print(f"Failed to fetch task status: {response.text}")
        time.sleep(10)  # Poll every 10 seconds







