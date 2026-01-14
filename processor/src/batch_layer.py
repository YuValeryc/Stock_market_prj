from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
TOPIC_NAME = os.environ.get('TOPIC_NAME', 'stock_prices')
HDFS_NAMENODE = os.environ.get('HDFS_NAMENODE', 'hdfs://namenode:9000')
HDFS_PATH = f"{HDFS_NAMENODE}/data/stock_prices"

# Define generic schema to capture everything as string first
# In a real scenario, full schema definition is better
schema = StructType([
    StructField("Exchange", StringType(), True),
    StructField("Stock Code", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Closing Price", StringType(), True),
    StructField("Opening Price", StringType(), True),
    StructField("Highest Price", StringType(), True),
    StructField("Lowest Price", StringType(), True),
    StructField("Volume", StringType(), True),
    StructField("timestamp", StringType(), True)
    # Add other fields as needed
])

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("StockBatchLayer") \
        .config("spark.cores.max", "1") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .load()

    # Parse JSON
    # For batch layer, we might want to store raw data or semi-processed
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Write to HDFS in Parquet format
    # Write to HDFS in Parquet format with Safe Mode Retry Logic
    # Checkpoints are crucial for exactly-once fault-tolerance
    checkpoint_location = f"{HDFS_NAMENODE}/checkpoints/stock_prices"

    # DEBUG: Clear checkpoint to force fresh start if needed (optional, good for dev recovery)
    # Using hadoop fs command via os.system for simplicity in this context
    # os.system(f"hadoop fs -rm -r {checkpoint_location}")
    # Actually, we can just change the checkpoint path to a new one to simulate fresh start
    checkpoint_location = f"{HDFS_NAMENODE}/checkpoints/stock_prices_v2"

    import time
    from py4j.protocol import Py4JJavaError

    while True:
        try:
            print("Attempting to start StreamingQuery...")
            query = parsed_df.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", HDFS_PATH) \
                .option("checkpointLocation", checkpoint_location) \
                .trigger(processingTime="1 minute") \
                .start()
            
            # Wait separately to catch runtime exceptions during execution
            query.awaitTermination()
            break 
        except Py4JJavaError as e:
            if "SafeModeException" in str(e):
                print("HDFS NameNode is in Safe Mode. Waiting 10 seconds...")
                time.sleep(10)
            else:
                raise e
        except Exception as e:
            print(f"Unexpected error: {e}")
            time.sleep(5)

    query.awaitTermination()
