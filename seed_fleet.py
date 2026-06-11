import mysql.connector
from config import DB_CONFIG

# Database configuration
db_config = DB_CONFIG

# 5 Mock drivers distributed across Bangalore near the center hub
mock_drivers = [
    {
        "id": "RIDER-ALPHA",
        "name": "Arjun Singh",
        "phone": "+919876543210",
        "vehicle": "Motorbike",
        "rating": 4.85,
        "lat": 12.9725,  # Very Close (~150 meters away)
        "lng": 77.5950
    },
    {
        "id": "RIDER-BETA",
        "name": "Priya Sharma",
        "phone": "+919876543211",
        "vehicle": "Scooter_Electric",
        "rating": 4.90,
        "lat": 12.9800,  # ~1.1 km away
        "lng": 77.6010
    },
    {
        "id": "RIDER-GAMMA",
        "name": "Amit Verma",
        "phone": "+919876543212",
        "vehicle": "Bicycle",  # Will incur a cargo penalty if items > 5
        "rating": 4.20,
        "lat": 12.9690,  # Very Close (~300 meters away)
        "lng": 77.5915
    },
    {
        "id": "RIDER-DELTA",
        "name": "Rajesh Kumar",
        "phone": "+919876543213",
        "vehicle": "Motorbike",
        "rating": 3.50,  # Low rating, will rank lower in composite score
        "lat": 12.9712,
        "lng": 77.5940
    },
    {
        "id": "RIDER-EPSILON",
        "name": "Vikram Malhotra",
        "phone": "+919876543214",
        "vehicle": "Motorbike",
        "rating": 4.95,
        "lat": 13.0800,  # Far away (~12km out - Outside the 10km search bubble)
        "lng": 77.6500
    }
]

def seed_system_fleet():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        print("⚡ Disabling temporary constraints for clean truncation...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("TRUNCATE TABLE active_driver_telemetry;")
        cursor.execute("TRUNCATE TABLE delivery_partners_drivers;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        
        insert_profile_query = """
            INSERT INTO delivery_partners_drivers (driver_id, full_name, phone_number, vehicle_type, is_available, rating_avg, compliance_status)
            VALUES (%s, %s, %s, %s, 1, %s, 'APPROVED');
        """
        
        insert_telemetry_query = """
            INSERT INTO active_driver_telemetry (driver_id, current_gps_location, heading_degrees, last_ping_time)
            VALUES (%s, ST_GeomFromText(%s, 4326), 0.0, NOW());
        """
        
        print("👥 Ingesting relational profiles and active spatial telemetry...")
        for driver in mock_drivers:
            # 1. Insert into Driver Directory Table
            cursor.execute(insert_profile_query, (
                driver["id"], driver["name"], driver["phone"], driver["vehicle"], driver["rating"]
            ))
            
            # 2. Insert into Spatial Telemetry Table
            wkt_point = f"POINT({driver['lat']} {driver['lng']})"
            cursor.execute(insert_telemetry_query, (driver["id"], wkt_point))
            
        conn.commit()
        print(f"✅ Successfully seeded {len(mock_drivers)} real-time tracking agents into the DBMS grid!")
        
    except mysql.connector.Error as err:
        print(f"❌ Database Seeding Failure: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    seed_system_fleet()
