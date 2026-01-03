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
    StructField("timestamp", StringType(), True)
    # Add other fields as needed
])

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("StockBatchLayer") \
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
    # Checkpoints are crucial for exactly-once fault-tolerance
    checkpoint_location = f"{HDFS_NAMENODE}/checkpoints/stock_prices"

    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", HDFS_PATH) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()
