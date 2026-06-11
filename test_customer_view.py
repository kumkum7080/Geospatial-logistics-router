import requests
import time

url = "http://127.0.0.1:8000/api/order/track/RIDER-0001"

print("📱 Opening Customer App View... Fetching live location tracking data from Redis cache.")
print("=" * 85)

# Simulate a customer checking the map 3 times over a few seconds
for check in range(1, 4):
    print(f"🔍 Map Refresh #{check}: Fetching latest coordinates...")
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            coords = data["telemetry"]
            print(f"📍 DRIVER LOCATED! -> Lat: {coords['latitude']}, Lng: {coords['longitude']} (Updated: {coords['last_updated']})")
        else:
            print(f"⚠️ Server Alert: {response.json().get('detail')}")
    except Exception as e:
        print(f"❌ Network Request Error: {e}")
        
    print("-" * 60)
    time.sleep(3) # Wait 3 seconds before refreshing again