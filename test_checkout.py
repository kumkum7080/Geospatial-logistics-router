import requests
import json

url = "http://127.0.0.1:8000/checkout"

# GPS Payload representing a client tracking request in Delhi
payload = {
    "customer_id": "CUST-0001",
    "latitude": 28.5250,
    "longitude": 77.2200,
    "items": [
        {"item_id": "PROD-001", "quantity": 2, "price_per_unit": 60.0},
        {"item_id": "MENU-001", "quantity": 1, "price_per_unit": 250.0}
    ]
}

print("📡 Sending authenticated checkout coordinates payload to FastAPI server...")
print("="*85)

try:
    response = requests.post(url, json=payload)
    print(f"📡 SERVER RESPONSE STATUS: {response.status_code}")
    print("\n📦 RESPONSE BODY INTERPRETED:")
    print(json.dumps(response.json(), indent=4))
except Exception as e:
    print(f"❌ Connection Error: {e}")
