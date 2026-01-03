from fastapi import FastAPI
import redis
import os
import json
import mlflow.sklearn
import pandas as pd
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
