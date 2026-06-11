import requests
import json
import time
import mysql.connector
from config import DB_CONFIG

BASE_URL = "http://127.0.0.1:8000"

def reset_db_and_fleet():
    """Resets database schemas and seeds basic fleet for batching test."""
    print("🧹 Resetting database and seeding fresh fleet for batching verification...")
    
    # 1. Connect to MySQL to reset telemetry
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursor.execute("TRUNCATE TABLE active_driver_telemetry;")
    cursor.execute("TRUNCATE TABLE delivery_partners_drivers;")
    cursor.execute("TRUNCATE TABLE orders;")
    cursor.execute("TRUNCATE TABLE orders_parent;")
    cursor.execute("TRUNCATE TABLE orders_sub_fulfillment;")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    
    # Seed 1 Motorbike driver and 1 Bicycle driver near Central Kitchen (28.6139, 77.2090)
    insert_profile = """
        INSERT INTO delivery_partners_drivers (driver_id, full_name, phone_number, vehicle_type, is_available, rating_avg, compliance_status)
        VALUES (%s, %s, %s, %s, 1, %s, 'APPROVED');
    """
    insert_telemetry = """
        INSERT INTO active_driver_telemetry (driver_id, current_gps_location, heading_degrees, last_ping_time, current_load_count)
        VALUES (%s, ST_GeomFromText(%s, 4326), 0.0, NOW(), 0);
    """
    
    cursor.execute(insert_profile, ("RIDER-BATCH-BIKE", "Arjun Singh", "+919876543210", "Motorbike", 4.9))
    cursor.execute(insert_telemetry, ("RIDER-BATCH-BIKE", "POINT(28.6145 77.2095)")) # Very close to merchant
    
    cursor.execute(insert_profile, ("RIDER-BATCH-CYCLE", "Priyanka Sen", "+919876543211", "Bicycle", 4.8))
    cursor.execute(insert_telemetry, ("RIDER-BATCH-CYCLE", "POINT(28.6140 77.2092)"))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Fleet and database reseeded successfully.")

def run_batching_test():
    reset_db_and_fleet()
    
    # Customer 1 Checkout: CUST-0001
    c1_payload = {
        "customer_id": "CUST-0001",
        "latitude": 28.5250,
        "longitude": 77.2200,
        "items": [
            {"item_id": "MENU-001", "quantity": 1, "price_per_unit": 250.0}
        ]
    }
    
    # Customer 2 Checkout: CUST-0002 (Co-located destination, same merchant)
    c2_payload = {
        "customer_id": "CUST-0002",
        "latitude": 28.5280,
        "longitude": 77.2230,
        "items": [
            {"item_id": "MENU-001", "quantity": 1, "price_per_unit": 250.0}
        ]
    }

    # Customer 3 Checkout: CUST-0003 (Co-located destination, same merchant)
    c3_payload = {
        "customer_id": "CUST-0003",
        "latitude": 28.5290,
        "longitude": 77.2250,
        "items": [
            {"item_id": "MENU-001", "quantity": 1, "price_per_unit": 250.0}
        ]
    }

    print("\n📦 SUBMITTING ORDER #1...")
    res1 = requests.post(f"{BASE_URL}/checkout", json=c1_payload)
    print(f"Status Code: {res1.status_code}")
    data1 = res1.json()
    print("RAW RESPONSE BODY:")
    print(json.dumps(data1, indent=2))
    rider1 = data1["geospatial_dispatch_matrix"]["assigned_delivery_rider"]
    driver_id1 = data1["geospatial_dispatch_matrix"]["assigned_driver_id"]
    is_batched1 = data1["geospatial_dispatch_matrix"]["is_batched_order"]
    print(f"-> Assigned Rider: {rider1} (Driver ID: {driver_id1})")
    print(f"-> Is Batched: {is_batched1}")

    # Wait 2 seconds
    time.sleep(2)

    print("\n📦 SUBMITTING ORDER #2 (Expect Stacking/Batching)...")
    res2 = requests.post(f"{BASE_URL}/checkout", json=c2_payload)
    print(f"Status Code: {res2.status_code}")
    data2 = res2.json()
    rider2 = data2["geospatial_dispatch_matrix"]["assigned_delivery_rider"]
    driver_id2 = data2["geospatial_dispatch_matrix"]["assigned_driver_id"]
    is_batched2 = data2["geospatial_dispatch_matrix"]["is_batched_order"]
    print(f"-> Assigned Rider: {rider2} (Driver ID: {driver_id2})")
    print(f"-> Is Batched: {is_batched2}")
    if is_batched2:
        print("-> Batching Sequence Info:")
        print(json.dumps(data2["geospatial_dispatch_matrix"]["batch_sequence_info"], indent=4))
        
    # Wait 2 seconds
    time.sleep(2)

    print("\n📦 SUBMITTING ORDER #3 (Expect Fallback as Rider is at Capacity)...")
    res3 = requests.post(f"{BASE_URL}/checkout", json=c3_payload)
    print(f"Status Code: {res3.status_code}")
    data3 = res3.json()
    rider3 = data3["geospatial_dispatch_matrix"]["assigned_delivery_rider"]
    driver_id3 = data3["geospatial_dispatch_matrix"]["assigned_driver_id"]
    is_batched3 = data3["geospatial_dispatch_matrix"]["is_batched_order"]
    print(f"-> Assigned Rider: {rider3} (Driver ID: {driver_id3})")
    print(f"-> Is Batched: {is_batched3}")

    print("\n⚡ TESTING BULK DISPATCH OPTIMIZER (/dispatch/optimize)...")
    # 1. Reset database and fleet first
    reset_db_and_fleet()
    
    # 2. Insert two fallback orders directly into MySQL database
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursor.execute("INSERT INTO orders_parent (order_id, customer_id, final_point, status_summary, total_transaction_amount) VALUES ('ORD-UNASSIGNED-1', 'CUST-0001', ST_GeomFromText('POINT(28.5250 77.2200)', 4326), 'Pending', 300.0)")
    cursor.execute("INSERT INTO orders_sub_fulfillment (sub_order_id, order_id, origin_merchant_id, current_workflow_status) VALUES ('SUB-UNASSIGNED-1', 'ORD-UNASSIGNED-1', 'RESTAURANT-KITCHEN-01', 'Pending')")
    cursor.execute("INSERT INTO orders (order_id, customer_id, hub_id, merchant_id, agent_id, total_amount, order_status) VALUES ('SUB-UNASSIGNED-1', 'CUST-0001', 'N/A', 'RESTAURANT-KITCHEN-01', 'DRIVER-FALLBACK-99', 300.0, 'PLACED')")
    
    cursor.execute("INSERT INTO orders_parent (order_id, customer_id, final_point, status_summary, total_transaction_amount) VALUES ('ORD-UNASSIGNED-2', 'CUST-0002', ST_GeomFromText('POINT(28.5280 77.2230)', 4326), 'Pending', 400.0)")
    cursor.execute("INSERT INTO orders_sub_fulfillment (sub_order_id, order_id, origin_merchant_id, current_workflow_status) VALUES ('SUB-UNASSIGNED-2', 'ORD-UNASSIGNED-2', 'RESTAURANT-KITCHEN-01', 'Pending')")
    cursor.execute("INSERT INTO orders (order_id, customer_id, hub_id, merchant_id, agent_id, total_amount, order_status) VALUES ('SUB-UNASSIGNED-2', 'CUST-0002', 'N/A', 'RESTAURANT-KITCHEN-01', 'DRIVER-FALLBACK-99', 400.0, 'PLACED')")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Seeded 2 unassigned orders in database queue.")

    # 3. Call /dispatch/optimize
    res_opt = requests.post(f"{BASE_URL}/dispatch/optimize")
    print(f"Status Code: {res_opt.status_code}")
    data_opt = res_opt.json()
    print("Optimization Response:")
    print(json.dumps(data_opt, indent=4))

if __name__ == "__main__":
    try:
        run_batching_test()
    except Exception as e:
        print(f"❌ Connection or Execution error: {e}")
