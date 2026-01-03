import os
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

# Configuration
MLFLOW_URI = os.environ.get('MLFLOW_URI', 'http://localhost:5000')
DATA_PATH = 'stock_price_data.csv' # In real usage, read from HDFS

def train():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("Stock_Price_Prediction")

    print(f"Loading data from {DATA_PATH}...")
    try:
        # Assuming we can access the CSV directly for this "training" simulation
        # In production, use HDFS client to read parquet
        df = pd.read_csv(DATA_PATH)
        
        # Preprocessing (Simplified)
        # Predict Closing Price based on Opening Price
        data = df[['Opening Price', 'Closing Price']].dropna()
        # Clean data - remove non-numeric
        data = data[pd.to_numeric(data['Opening Price'], errors='coerce').notnull()]
        data = data[pd.to_numeric(data['Closing Price'], errors='coerce').notnull()]
        
        X = data[['Opening Price']]
        y = data['Closing Price']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        with mlflow.start_run():
            model = LinearRegression()
            model.fit(X_train, y_train)

            predictions = model.predict(X_test)
            mse = mean_squared_error(y_test, predictions)

            mlflow.log_metric("mse", mse)
            mlflow.sklearn.log_model(model, "model")
            
            print(f"Model trained. MSE: {mse}")
            print(f"Model URI: {mlflow.get_artifact_uri('model')}")

    except Exception as e:
        print(f"Error during training: {e}")

if __name__ == "__main__":
    train()
