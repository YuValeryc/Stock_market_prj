from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

import os
import json
import redis
import pandas as pd
import fsspec

import mlflow
import mlflow.pyfunc

# =====================
# APP INIT
# =====================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =====================
# CONFIG
# =====================
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
MLFLOW_URI = os.environ.get("MLFLOW_URI", "http://mlflow-server:5000")
MODEL_NAME = "StockPriceModel"

# =====================
# REDIS INIT
# =====================
try:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=0,
        decode_responses=True
    )
    r.ping()
    print("✅ Redis connected")
except Exception as e:
    r = None
    print("❌ Redis not connected:", e)

# =====================
# MLFLOW CONFIG
# =====================
mlflow.set_tracking_uri(MLFLOW_URI)

# Global cache
model = None

# =====================
# LAZY LOAD MODEL
# =====================
def get_model():
    """
    Load model lazily when first needed.
    Cache model in memory after successful load.
    """
    global model

    if model is not None:
        return model

    try:
        print("⏳ Trying to load ML model from MLflow (@production)...")
        model = mlflow.pyfunc.load_model(
            f"models:/{MODEL_NAME}@production"
        )
        print(f"✅ ML model '{MODEL_NAME}' loaded successfully")
        return model
    except Exception as e:
        print("⚠️ Model not ready yet:", e)
        return None

# =====================
# SCHEMAS
# =====================
class PredictionRequest(BaseModel):
    opening_price: float
    symbol: Optional[str] = None

# =====================
# ROUTES
# =====================
@app.get("/")
def root():
    return {"message": "Stock Market API is running"}

# ---------- REALTIME PRICE ----------
@app.get("/latest/{stock_code}")
def get_latest_stock_price(stock_code: str):
    if not r:
        return {"error": "Redis unavailable"}

    data = r.get(stock_code)
    if data:
        return json.loads(data)

    return {"error": "Stock code not found"}

# ---------- PREDICT ----------
@app.post("/predict")
def predict_price(request: PredictionRequest):
    mdl = get_model()

    if mdl is None:
        return {
            "error": "Model not ready yet. Please train the model first."
        }

    try:
        input_df = pd.DataFrame([{
            "opening_price": request.opening_price
        }])

        prediction = mdl.predict(input_df)

        return {
            "symbol": request.symbol,
            "opening_price": request.opening_price,
            "predicted_closing_price": float(prediction.iloc[0])
        }

    except Exception as e:
        return {"error": str(e)}

# ---------- MODEL STATUS (OPTIONAL - DEBUG) ----------
@app.get("/model/status")
def model_status():
    return {
        "loaded": model is not None
    }

# ---------- HISTORY FROM HDFS ----------
@app.get("/history")
def get_historical_data(
    stock_code: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
):
    try:
        fs = fsspec.filesystem(
            "webhdfs",
            host="namenode",
            port=9870
        )

        df = pd.read_parquet(
            "/data/stock_prices",
            filesystem=fs
        )

        if stock_code:
            df = df[df["Stock Code"] == stock_code]

        if start_date:
            df = df[df["Date"] >= start_date]

        if end_date:
            df = df[df["Date"] <= end_date]

        if "timestamp" in df.columns:
            df = df.sort_values(by="timestamp", ascending=False)

        return df.to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}
