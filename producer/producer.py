import os
import time
import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'stock_prices')
CSV_FILE_PATH = 'stock_price_data.csv'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=json_serializer
            )
            print("Connected to Kafka!")
            return producer
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def run_producer():
    producer = create_producer()
    
    # Check if file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"File {CSV_FILE_PATH} not found.")
        return

    print(f"Reading data from {CSV_FILE_PATH}...")
    # Read CSV. Skipping the first row which seems to be metadata if needed, checking structure
    # Based on file view: Line 1 header, Line 2 start of data.
    try:
        df = pd.read_csv(CSV_FILE_PATH) # Pandas should handle the header automatically
        # Clean column names: remove quotes, extra spaces
        df.columns = [c.strip().replace('"', '').replace('\t', '') for c in df.columns]
        print(f"Columns found: {df.columns.tolist()}")
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    print("Starting stream...")
    
    while True:
        # Simulate streaming
        for index, row in df.iterrows():
            try:
                # Convert row to dict
                data = row.to_dict()
                # Add timestamp for streaming simulation
                data['timestamp'] = datetime.now().isoformat()
                
                # Send to Kafka
                producer.send(TOPIC_NAME, data)
                
                # Print occasionally
                if index % 100 == 0:
                    print(f"Sent message {index}: {data.get('Stock Code', 'N/A')}")
                    print(f'Data: {data}')
                
                # Sleep to simulate realtime - adjust as needed
                time.sleep(0.02) # Slower to be easier to follow logs
                
            except Exception as e:
                print(f"Error sending message: {e}")
        
        print("Finished one pass of CSV, restarting...")

if __name__ == "__main__":
    run_producer()
