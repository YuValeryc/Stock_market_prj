from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'stock_prices')
REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = '6379'

print(f"--- CONFIG DEBUG ---")
print(f"KAFKA_BROKER: '{KAFKA_BROKER}'")
print(f"TOPIC_NAME: '{TOPIC_NAME}'")
print(f"REDIS_HOST: '{REDIS_HOST}'")
print(f"--------------------")

# Define Schema based on the CSV structure we saw
# Exchange, No., Date, Stock Code, Reference Price, Opening Price, Closing Price, Highest Price, Lowest Price, Average Price, ...
# We'll select a few key fields for the speed layer
schema = StructType([
    StructField("Exchange", StringType(), True),
    StructField("Stock Code", StringType(), True), # Note the space in CSV header
    StructField("Closing Price", StringType(), True), # Reading as string first to handle potential formatting
    StructField("timestamp", StringType(), True)
])

def process_batch(df, epoch_id):
    # This function is called for each micro-batch
    # Write to Redis
    # We use a simple approach: using redis-py inside a foreachPartition or similar, 
    # but for simplicity in Spark Structured Streaming, we can use foreach.
    
    print(f"Batch with {df.count()} records")
    # Show some data for debugging
    df.show(5)

    def write_to_redis(rows):
        import redis
        import json
        # Add error handling connection
        try:
           r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), db=0)
        except Exception as e:
           print(f"Redis Connection Error: {e}")
           return

        count = 0
        for row in rows:
            try:
                if row['Stock Code']:
                    # Key: stock_code, Value: JSON of the row
                    r.set(row['Stock Code'], json.dumps(row.asDict()))
                    count += 1
            except Exception as e:
                print(f"Error processing row: {e}")
        print(f"Written {count} records to Redis in this partition")
    
    df.foreachPartition(write_to_redis)

import time
import socket

def wait_for_service(host, port, timeout=60):
    start_time = time.time()
    while True:
        try:
            with socket.create_connection((host, int(port)), timeout=2):
                print(f"Service {host}:{port} is reachable.")
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            pass
        
        if time.time() - start_time > timeout:
            print(f"Timeout waiting for {host}:{port}")
            return False
            
        print(f"Waiting for {host}:{port}...")
        time.sleep(2)

if __name__ == "__main__":
    host, port = KAFKA_BROKER.split(':')
    wait_for_service(host, port)

    spark = SparkSession.builder \
        .appName("StockSpeedLayer") \
        .config("spark.cores.max", "1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Start Query
    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .start()

    query.awaitTermination()
