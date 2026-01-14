import os
import mlflow
import mlflow.spark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# =====================
# CONFIG
# =====================
MLFLOW_URI = os.environ.get("MLFLOW_URI", "http://mlflow-server:5000")
HDFS_PATH = "hdfs://namenode:9000/data/stock_prices"
EXPERIMENT_NAME = "Stock_Price_Prediction_Spark"

print("========== CONFIG ==========")
print(f"MLFLOW_URI   = {MLFLOW_URI}")
print(f"HDFS_PATH    = {HDFS_PATH}")
print(f"EXPERIMENT   = {EXPERIMENT_NAME}")
print("============================")

# =====================
# SPARK SESSION
# =====================
spark = SparkSession.builder \
    .appName("StockPriceSparkML") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark session started")
print(f"Spark version: {spark.version}")

# =====================
# LOAD DATA FROM HDFS
# =====================
print("Loading data from HDFS...")
# Check if path exists logic could be added here, but relying on Spark failure for now if missing
try:
    df = spark.read.parquet(HDFS_PATH)
except Exception as e:
    print(f"Error reading from HDFS path {HDFS_PATH}: {e}")
    # Fallback for testing/empty state or exit?
    # For now, let's assumes it might fail if Empty, so we'll just stop.
    spark.stop()
    exit(1)

print(f"Total rows in raw data: {df.count()}")
print("Schema:")
df.printSchema()

# =====================
# BASIC CLEANING
# =====================
df = df.select(
    col("Opening Price").cast("double").alias("opening_price"),
    col("Closing Price").cast("double").alias("closing_price")
).dropna()

print(f"Rows after cleaning: {df.count()}")

# =====================
# FEATURE ENGINEERING
# =====================
assembler = VectorAssembler(
    inputCols=["opening_price"],
    outputCol="features"
)

data = assembler.transform(df).select("features", "closing_price")

print("Sample feature rows:")
data.show(5, truncate=False)

# =====================
# TRAIN / TEST SPLIT
# =====================
train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)

train_count = train_df.count()
test_count = test_df.count()

print(f"Train rows: {train_count}")
print(f"Test rows : {test_count}")

if train_count == 0 or test_count == 0:
    print("❌ ERROR: Train or test dataset is empty. Exiting early.")
    spark.stop()
    exit(1)

# =====================
# MLFLOW SETUP
# =====================
print("Setting up MLflow...")
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT_NAME)

# =====================
# TRAIN MODEL
# =====================
with mlflow.start_run(run_name="LinearRegression_SparkML"):
    print("Training Linear Regression model...")

    lr = LinearRegression(
        featuresCol="features",
        labelCol="closing_price"
    )

    model = lr.fit(train_df)

    print("Model training completed")

    # =====================
    # EVALUATION
    # =====================
    predictions = model.transform(test_df)

    print("Sample predictions:")
    predictions.select("closing_price", "prediction").show(5, truncate=False)

    evaluator = RegressionEvaluator(
        labelCol="closing_price",
        predictionCol="prediction",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)

    print(f"RMSE = {rmse}")

    # =====================
    # LOG TO MLFLOW
    # =====================
    mlflow.log_metric("rmse", rmse)
    mlflow.spark.log_model(model, "model")

    print("Model logged to MLflow")

print("Training job finished successfully")
spark.stop()
