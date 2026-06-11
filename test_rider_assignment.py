import mysql.connector
from datetime import datetime
from config import DB_CONFIG

db_config = DB_CONFIG

def setup_and_assign_rider():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        print("🧹 Clearing old rider profiles and telemetry logs for clean simulation...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("TRUNCATE TABLE active_driver_telemetry;")
        cursor.execute("TRUNCATE TABLE delivery_partners_drivers;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        print("🚴 Seeding Active Delivery Fleet around Delhi in production tables...")
        insert_profile_query = """
            INSERT INTO delivery_partners_drivers (driver_id, full_name, phone_number, vehicle_type, is_available, rating_avg, compliance_status)
            VALUES (%s, %s, %s, %s, %s, %s, 'APPROVED');
        """
        
        insert_telemetry_query = """
            INSERT INTO active_driver_telemetry (driver_id, current_gps_location, heading_degrees, last_ping_time)
            VALUES (%s, ST_GeomFromText(%s, 4326), 0.0, NOW());
        """
        
        # Seeding drivers at different coordinates relative to Central Kitchen (28.5300, 77.2150)
        drivers_data = [
            ("RIDER-0001", "Rider Alpha (Super Close)", "+1111", "Motorbike", 1, 4.9),
            ("RIDER-0002", "Rider Bravo (Mid Distance)", "+2222", "Motorbike", 1, 4.8),
            ("RIDER-0003", "Rider Charlie (Far Away)", "+3333", "Bicycle", 1, 4.2),
            ("RIDER-0004", "Rider Delta (Busy/Unavailable)", "+4444", "Motorbike", 0, 4.5) # Super close but unavailable
        ]
        
        for d in drivers_data:
            cursor.execute(insert_profile_query, (d[0], d[1], d[2], d[3], d[4], d[5]))
        
        telemetry_data = [
            ("RIDER-0001", "POINT(28.5290 77.2140)"),
            ("RIDER-0002", "POINT(28.5420 77.2280)"),
            ("RIDER-0003", "POINT(28.5700 77.1900)"),
            ("RIDER-0004", "POINT(28.5295 77.2145)")
        ]
        
        for t in telemetry_data:
            cursor.execute(insert_telemetry_query, (t[0], t[1]))
            
        conn.commit()
        print("✅ Delivery agents and spatial telemetry live in database.")

        # TARGET: Find closest available driver for Central Kitchen Delhi 01 (POINT(28.5300 77.2150))
        restaurant_wkt = "POINT(28.5300 77.2150)"
        restaurant_name = "Central Kitchen Delhi 01"
        
        print(f"\n📡 Executing Live Assignment Match for order pickup at: '{restaurant_name}'...")
        print("="*85)

        assignment_query = """
            SELECT 
                active_driver_telemetry.driver_id, 
                delivery_partners_drivers.full_name as name, 
                delivery_partners_drivers.vehicle_type,
                ST_Distance_Sphere(active_driver_telemetry.current_gps_location, ST_GeomFromText(%s, 4326)) AS distance_meters
            FROM active_driver_telemetry
            JOIN delivery_partners_drivers ON active_driver_telemetry.driver_id = delivery_partners_drivers.driver_id
            WHERE delivery_partners_drivers.is_available = 1
            ORDER BY distance_meters ASC
            LIMIT 1;
        """
        
        cursor.execute(assignment_query, (restaurant_wkt,))
        best_rider = cursor.fetchone()
        
        if best_rider:
            distance_km = best_rider['distance_meters'] / 1000.0
            print("\n🚨 MATCH FOUND! ORDER DISPATCHED SYSTEM-WIDE:")
            print(f"  👤 Assigned Driver : {best_rider['name']} ({best_rider['driver_id']})")
            print(f"  🚲 Vehicle Fleet   : {best_rider['vehicle_type']}")
            print(f"  📐 Distance to Shop: {best_rider['distance_meters']:.1f} meters ({distance_km:.2f} km)")
            print(f"  ⏱️ Est. Pickup ETA : {max(1, int(best_rider['distance_meters'] / 250))} minutes")
        else:
            print("  ❌ CRITICAL: No available delivery agents online inside the operational radius!")

    except mysql.connector.Error as err:
        print(f"❌ Rider Assignment Transaction Failed: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    setup_and_assign_rider()
