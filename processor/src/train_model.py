import os
import mlflow
import mlflow.pyfunc
import pandas as pd

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
REGISTERED_MODEL_NAME = "StockPriceModel"

# =====================
# SPARK SESSION
# =====================
spark = SparkSession.builder \
    .appName("StockPriceSparkML") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================
# LOAD DATA
# =====================
df = spark.read.parquet(HDFS_PATH)

df = df.select(
    col("Opening Price").cast("double").alias("opening_price"),
    col("Closing Price").cast("double").alias("closing_price")
).dropna()

# =====================
# FEATURE ENGINEERING
# =====================
assembler = VectorAssembler(
    inputCols=["opening_price"],
    outputCol="features"
)

data = assembler.transform(df).select("features", "closing_price")

train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)

# =====================
# MLFLOW SETUP
# =====================
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT_NAME)

# =====================
# PYFUNC MODEL (KHÔNG DÙNG SPARK)
# =====================
class LinearFormulaModel(mlflow.pyfunc.PythonModel):
    def __init__(self, coef, intercept):
        self.coef = coef
        self.intercept = intercept

    def predict(self, context, model_input: pd.DataFrame):
        return model_input["opening_price"] * self.coef + self.intercept

# =====================
# TRAIN MODEL
# =====================
with mlflow.start_run(run_name="LinearRegression_SparkML"):
    lr = LinearRegression(
        featuresCol="features",
        labelCol="closing_price"
    )

    model = lr.fit(train_df)

    predictions = model.transform(test_df)

    evaluator = RegressionEvaluator(
        labelCol="closing_price",
        predictionCol="prediction",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)
    mlflow.log_metric("rmse", rmse)

    # ===== LẤY HỆ SỐ =====
    coef = float(model.coefficients[0])
    intercept = float(model.intercept)

    mlflow.log_param("coef", coef)
    mlflow.log_param("intercept", intercept)

    # ===== LOG PYFUNC (AN TOÀN 100%) =====
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=LinearFormulaModel(coef, intercept),
        registered_model_name=REGISTERED_MODEL_NAME
    )

    print(f" Model registered: {REGISTERED_MODEL_NAME}")
    print(f"   closing_price = {coef} * opening_price + {intercept}")

spark.stop()
