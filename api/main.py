from fastapi import FastAPI
import redis
import os
import json
import mlflow.sklearn
import pandas as pd
import fsspec
from pydantic import BaseModel

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
MLFLOW_URI = os.environ.get('MLFLOW_URI', 'http://mlflow-server:5000')

# Initialize Redis
try:
    r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT), db=0, decode_responses=True)
except:
    r = None
    print("Redis not connected")

# Set MLflow URI
mlflow.set_tracking_uri(MLFLOW_URI)

class PredictionRequest(BaseModel):
    opening_price: float

@app.get("/")
def read_root():
    return {"message": "Stock Market API is running"}

@app.get("/latest/{stock_code}")
def get_latest_stock_price(stock_code: str):
    if not r:
        return {"error": "Redis unavailable"}
    
    data = r.get(stock_code)
    if data:
        return json.loads(data)
    else:
        return {"error": "Stock code not found"}

@app.post("/predict")
def predict_price(request: PredictionRequest):
    # Load latest production model
    # For simplicity, we assume a specific run or just try to load a model if registered
    # Here we just mock the prediction for stability if no model exists yet
    try:
        # model = mlflow.sklearn.load_model("models:/StockPriceModel/Production")
        # prediction = model.predict([[request.opening_price]])
        # return {"predicted_closing_price": prediction[0]}
        
        # Mock logic matching the linear regression idea (dummy)
        return {"predicted_closing_price": request.opening_price * 1.01, "note": "Mock prediction (train model to get real)"}
    except Exception as e:
        return {"error": str(e)}

HDFS_NAMENODE_HTTP = os.environ.get('HDFS_NAMENODE_HTTP', 'http://namenode:9870')

@app.get("/history")
def get_historical_data(stock_code: str = None, start_date: str = None, end_date: str = None):
    try:
        # Construct HDFS path
        # Using webhdfs via fsspec
        hdfs_path = "/data/stock_prices"
        
        # Read parquet file directly from HDFS
        # Note: In a real production system with large data, we should not read the entire dataset.
        # Ideally, we would rely on Spark or Hive to query, or allow partition pruning here.
        # For this demo, we assume the data volume is manageable or we are just reading recent files.
        try:
            # Simple retry loop for SafeMode or connectivity issues
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    df = pd.read_parquet(
                        hdfs_path, 
                        filesystem=fsspec.filesystem("webhdfs", host="namenode", port=9870)
                    )
                    break # Success
                except Exception as inner_e:
                    if "SafeMode" in str(inner_e) or "Safe mode" in str(inner_e):
                         if attempt < max_retries - 1:
                             print(f"HDFS in Safe Mode, retrying ({attempt+1}/{max_retries})...")
                             # Trigger a leave if we can? No, API shouldn't control infra. just wait.
                             # Actually we can't easily fix it from here, but we can wait.
                             import time
                             time.sleep(2)
                             continue
                    raise inner_e
        except Exception as e:
            print(f"Error reading from HDFS: {e}")
            # Fallback for local testing or empty HDFS
            return {"data": [], "error": f"Could not read from HDFS: {str(e)}"}

        # Filter data
        if stock_code:
            df = df[df['Stock Code'] == stock_code]
        
        if start_date:
            df = df[df['Date'] >= start_date]
            
        if end_date:
            df = df[df['Date'] <= end_date]
            
        # Sort by date descending
        if not df.empty and 'timestamp' in df.columns:
             df = df.sort_values(by='timestamp', ascending=False)
             
        # Convert to records
        result = df.to_dict(orient="records")
        return result
    except Exception as e:
        return {"error": str(e)}
