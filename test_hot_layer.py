import redis
from config import REDIS_CONFIG

try:
    # Connect to the local hot Redis layer running in Docker
    r = redis.Redis(**REDIS_CONFIG)
    
    # Corrected syntax: Pass longitude, latitude, and member name as explicit parameters
    r.geoadd("active_driver_locations", (-74.0048, 40.7418, "V-VAN-201"))
    r.geoadd("active_driver_locations", (-73.9714, 40.7023, "V-BIKE-101"))
    print("⚡ [Redis Hot Layer] Successfully wrote live GPS coordinates to RAM!")
    
    # Compute the distance between them in meters
    distance_meters = r.geodist("active_driver_locations", "V-VAN-201", "V-BIKE-101", unit="m")
    print(f"📏 [Redis Distance Engine] Distance between Van and Bike: {distance_meters:.2f} meters")

except Exception as e:
    print(f"❌ Redis test failed: {e}")
