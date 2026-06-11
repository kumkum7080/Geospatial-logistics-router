import time
import json
from confluent_kafka import Producer, Consumer, KafkaError

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "driver-telemetry"

# --- PART 1: THE PRODUCER (Vehicle GPS Tracker) ---
def delivery_vehicle_simulator():
    print("\n🚚 [Producer] Activating GPS transmitter on vehicle 'V-VAN-201'...")
    
    # Configure Kafka Producer connection
    p_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(p_config)
    
    # Simulated delivery route points (moving step-by-step through NYC)
    simulated_route = [
        {"lat": 40.7418, "lon": -74.0048, "speed_kmh": 0},   # Starting at Hub
        {"lat": 40.7435, "lon": -74.0012, "speed_kmh": 32},  # En route stop 1
        {"lat": 40.7461, "lon": -73.9965, "speed_kmh": 45},  # En route stop 2
        {"lat": 40.7502, "lon": -73.9910, "speed_kmh": 15}   # Approaching customer
    ]
    
    for i, gps_ping in enumerate(simulated_route):
        payload = {
            "vehicle_id": "V-VAN-201",
            "latitude": gps_ping["lat"],
            "longitude": gps_ping["lon"],
            "speed_kmh": gps_ping["speed_kmh"],
            "timestamp": time.time()
        }
        
        # Convert dictionary data to a JSON byte string for network delivery
        producer.produce(
            TOPIC_NAME, 
            key=payload["vehicle_id"], 
            value=json.dumps(payload).encode('utf-8')
        )
        print(f"📡 [GPS Ping {i+1}] Broadcasted coordinates: ({payload['latitude']}, {payload['longitude']}) @ {payload['speed_kmh']} km/h")
        producer.flush() # Force transmission over the local network pipe
        time.sleep(1) # Wait 1 second between pings

# --- PART 2: THE CONSUMER (Central Logistics Receiver) ---
def logistics_center_receiver():
    print("\n🏢 [Consumer] Initializing Central Logistics Intake Stream...")
    
    c_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'logistics-core-engine',
        'auto.offset.reset': 'earliest' # Read stream from the very beginning of the trip
    }
    consumer = Consumer(c_config)
    consumer.subscribe([TOPIC_NAME])
    
    print("📥 [Listening] Awaiting telemetry packets from Kafka conveyor belt...\n")
    pings_collected = 0
    
    try:
        while pings_collected < 4:
            msg = consumer.poll(timeout=2.0) # Check stream channel for new packets
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka Stream Error: {msg.error()}")
                    break
                    
            # Decode binary network packets back into Python text data
            data = json.loads(msg.value().decode('utf-8'))
            print(f"📥 [Processed Log] Received from {data['vehicle_id']} -> Lat: {data['latitude']}, Lon: {data['longitude']} [Stored securely in Stream Log]")
            pings_collected += 1
            
    finally:
        consumer.close()

if __name__ == "__main__":
    # 1. Fire up the vehicle transmitter to broadcast data into Kafka
    delivery_vehicle_simulator()
    
    # 2. Fire up the intake receiver to process data out of Kafka
    logistics_center_receiver()