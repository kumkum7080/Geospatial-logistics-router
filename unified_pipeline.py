import time
import json
import redis
import mysql.connector
from confluent_kafka import Producer, Consumer
from config import DB_CONFIG, REDIS_CONFIG

# --- CONNECTIONS & CONFIGS ---
KAFKA_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'delivery-settlement-stream'

# 1. Initialize Hot Layer Connection
r_cache = redis.Redis(**REDIS_CONFIG)

# 2. Initialize MySQL Warm Layer Connection
db_conn = mysql.connector.connect(**DB_CONFIG)
cursor = db_conn.cursor()

# --- STEP 1: SIMULATE THE ACTIVE DRIVER DISPATCH (Pumping to Hot & Streaming) ---
def simulate_active_delivery():
    print("\n🚀 [System] Starting active delivery run for Order #5001...")
    
    kafka_producer = Producer({'bootstrap.servers': KAFKA_SERVERS})
    vehicle_id = "V-VAN-201"
    
    # Simple coordinates representing progress along a route
    gps_route = [
        {"lat": 40.7418, "lon": -74.0048}, # Starting point
        {"lat": 40.7502, "lon": -73.9910}  # Final Destination
    ]
    
    for idx, coord in enumerate(gps_route):
        # Update the HOT cache layer instantly for real-time map tracking
        r_cache.geoadd("live_tracking_map", (coord["lon"], coord["lat"], vehicle_id))
        print(f"⚡ [Hot Cache Updated] {vehicle_id} location saved in memory.")
        
        # Stream the packet to Kafka for historical queuing
        telemetry_packet = {
            "order_id": 5001,
            "vehicle_id": vehicle_id,
            "latitude": coord["lat"],
            "longitude": coord["lon"],
            "step": idx + 1,
            "is_final": (idx == len(gps_route) - 1)
        }
        
        kafka_producer.produce(
            KAFKA_TOPIC, 
            key=str(telemetry_packet["order_id"]), 
            value=json.dumps(telemetry_packet).encode('utf-8')
        )
        kafka_producer.flush()
        time.sleep(1) # Simulating movement interval

# --- STEP 2: SETTLEMENT CONSUMER (Reading Stream & Offloading to MySQL Warm Storage) ---
# --- STEP 2: SETTLEMENT CONSUMER (Reading Stream & Offloading to MySQL Warm Storage) ---
def process_settlement_stream():
    print("\n🏢 [Settlement Service] Listening for completed trips on Kafka...")
    
    kafka_consumer = Consumer({
        'bootstrap.servers': KAFKA_SERVERS,
        'group.id': 'settlement-workers-v2',  # Changed group ID to read fresh offsets
        'auto.offset.reset': 'earliest'
    })
    kafka_consumer.subscribe([KAFKA_TOPIC])
    
    try:
        while True:
            msg = kafka_consumer.poll(timeout=1.0) # Check the stream channel
            if msg is None:
                # Instead of shutting down immediately, let's keep waiting for the data packet
                continue
            if msg.error():
                print(f"❌ Kafka Error: {msg.error()}")
                break
                
            data = json.loads(msg.value().decode('utf-8'))
            print(f"📥 [Stream Intake] Processing data packet for Step #{data['step']}")
            
            # If this coordinate is marked as the final destination, write it permanently to MySQL!
            if data["is_final"]:
                print(f"🏁 [Trip Complete] Order #{data['order_id']} has reached its final destination.")
                
                try:
                    update_query = """
                        INSERT INTO historical_delivery_ledger (order_id, vehicle_id, final_lat, final_lon, status)
                        VALUES (%s, %s, %s, %s, 'DELIVERED')
                        ON DUPLICATE KEY UPDATE status='DELIVERED';
                    """
                    cursor.execute(update_query, (data["order_id"], data["vehicle_id"], data["latitude"], data["longitude"]))
                    db_conn.commit()
                    print("💾 [Warm Storage Settled] Trip data saved permanently to MySQL disk ledger.")
                    
                    # Clean up the Hot Cache since the trip is over to save RAM space
                    r_cache.zrem("live_tracking_map", data["vehicle_id"])
                    print("🧹 [Hot Cache Purged] Cleared live memory footprint for finished driver.\n")
                    
                    # Break out of the infinite loop now that the trip is fully settled!
                    break
                    
                except mysql.connector.Error as err:
                    print(f"❌ MySQL Storage failed: {err}")
                    break
    finally:
        kafka_consumer.close()

if __name__ == "__main__":
    simulate_active_delivery()
    process_settlement_stream()
    
    # Close database connections cleanly
    cursor.close()
    db_conn.close()
