from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Stock Market API is running"}

def test_predict():
    response = client.post("/predict", json={"opening_price": 100.0})
    assert response.status_code == 200
    assert "predicted_closing_price" in response.json()
