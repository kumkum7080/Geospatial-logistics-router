import pymysql
import time
import random
import math
from redis import Redis
from config import DB_CONFIG, REDIS_CONFIG

# Database configuration
DB_HOST = DB_CONFIG["host"]
DB_PORT = DB_CONFIG["port"]
DB_USER = DB_CONFIG["user"]
DB_PASSWORD = DB_CONFIG["password"]
DB_NAME = DB_CONFIG["database"]

# Multi-City Scale Parameters
RESTAURANTS_PER_CITY = 100  # 100 stores * 99 cities = 9,900 Total Restaurants
DRIVERS_PER_CITY = 30       # 30 riders * 99 cities = 2,970 Total Active Drivers

# Connect to running Redis cache engine
try:
    redis_client = Redis(**REDIS_CONFIG)
    redis_client.ping()
    print("🟢 System Connected to Redis Live Cache Successfully.")
except Exception as e:
    print(f"❌ Redis Connection Failure: {e}")
    exit(1)

# Cleaned Indian Smart Cities list with exact baseline coordinate scales
SMART_CITIES = [
    {"city": "Delhi", "lat": 28.6139, "lng": 77.2090, "suffix": "DEL"},
    {"city": "Indore", "lat": 22.7196, "lng": 75.8577, "suffix": "IND"},
    {"city": "Mumbai", "lat": 19.0760, "lng": 72.8777, "suffix": "BOM"},
    {"city": "Bangalore", "lat": 12.9716, "lng": 77.5946, "suffix": "BLR"},
    {"city": "Kolkata", "lat": 22.5726, "lng": 88.3639, "suffix": "CCU"},
    {"city": "Bhopal", "lat": 23.2599, "lng": 77.4126, "suffix": "BHO"},
    {"city": "Jabalpur", "lat": 23.1815, "lng": 79.9864, "suffix": "JAB"},
    {"city": "Gwalior", "lat": 26.2183, "lng": 78.1828, "suffix": "GWL"},
    {"city": "Ujjain", "lat": 23.1760, "lng": 75.7885, "suffix": "UJJ"},
    {"city": "Pune", "lat": 18.5204, "lng": 73.8567, "suffix": "PNQ"},
    {"city": "Nagpur", "lat": 21.1458, "lng": 79.0882, "suffix": "NAG"},
    {"city": "Thane", "lat": 19.2183, "lng": 72.9781, "suffix": "THA"},
    {"city": "Ahmedabad", "lat": 23.0225, "lng": 72.5714, "suffix": "AMD"},
    {"city": "Surat", "lat": 21.1702, "lng": 72.8311, "suffix": "SUR"},
    {"city": "Vadodara", "lat": 22.3072, "lng": 73.1812, "suffix": "BDQ"},
    {"city": "Rajkot", "lat": 22.3039, "lng": 70.8022, "suffix": "RAJ"},
    {"city": "Jaipur", "lat": 26.9124, "lng": 75.7873, "suffix": "JAI"},
    {"city": "Udaipur", "lat": 24.5854, "lng": 73.7125, "suffix": "UDR"},
    {"city": "Kota", "lat": 25.2138, "lng": 75.8648, "suffix": "KOT"},
    {"city": "Ajmer", "lat": 26.4491, "lng": 74.6373, "suffix": "AJM"},
    {"city": "Lucknow", "lat": 26.8467, "lng": 80.9462, "suffix": "LKO"},
    {"city": "Kanpur", "lat": 26.4499, "lng": 80.3319, "suffix": "KNP"},
    {"city": "Varanasi", "lat": 25.3176, "lng": 82.9739, "suffix": "VNS"},
    {"city": "Agra", "lat": 27.1767, "lng": 78.0081, "suffix": "AGR"},
    {"city": "Prayagraj", "lat": 25.4358, "lng": 81.8463, "suffix": "ALD"},
    {"city": "Aligarh", "lat": 27.8974, "lng": 78.0880, "suffix": "ALG"},
    {"city": "Jhansi", "lat": 25.4484, "lng": 78.5685, "suffix": "JHS"},
    {"city": "Patna", "lat": 25.5941, "lng": 85.1376, "suffix": "PAT"},
    {"city": "Muzaffarpur", "lat": 26.1197, "lng": 85.3910, "suffix": "MZF"},
    {"city": "Bhagalpur", "lat": 25.2425, "lng": 87.0135, "suffix": "BGP"},
    {"city": "Ranchi", "lat": 23.3441, "lng": 85.3096, "suffix": "IXR"},
    {"city": "Jamshedpur", "lat": 22.8046, "lng": 86.2029, "suffix": "IXW"},
    {"city": "Cuttack", "lat": 20.4625, "lng": 85.8830, "suffix": "CTC"},
    {"city": "Rourkela", "lat": 22.2604, "lng": 84.8536, "suffix": "RUK"},
    {"city": "Hyderabad", "lat": 17.3850, "lng": 78.4867, "suffix": "HYD"},
    {"city": "Warangal", "lat": 17.9689, "lng": 79.5941, "suffix": "WGL"},
    {"city": "Karimnagar", "lat": 18.4386, "lng": 79.1288, "suffix": "KRM"},
    {"city": "Chennai", "lat": 13.0827, "lng": 80.2707, "suffix": "MAA"},
    {"city": "Coimbatore", "lat": 11.0168, "lng": 76.9558, "suffix": "CJB"},
    {"city": "Madurai", "lat": 9.9252, "lng": 78.1198, "suffix": "IXM"},
    {"city": "Salem", "lat": 11.6643, "lng": 78.1460, "suffix": "SLM"},
    {"city": "Vellore", "lat": 12.9165, "lng": 79.1325, "suffix": "VEL"},
    {"city": "Tiruchirappalli", "lat": 10.7905, "lng": 78.7047, "suffix": "TRZ"},
    {"city": "Thiruvananthapuram", "lat": 8.5241, "lng": 76.9366, "suffix": "TRV"},
    {"city": "Kochi", "lat": 9.9312, "lng": 76.2673, "suffix": "COK"},
    {"city": "Kozhikode", "lat": 11.2588, "lng": 75.7804, "suffix": "CCJ"},
    {"city": "Visakhapatnam", "lat": 17.6868, "lng": 83.2185, "suffix": "VTZ"},
    {"city": "Vijayawada", "lat": 16.5062, "lng": 80.6480, "suffix": "VGA"},
    {"city": "Tirupati", "lat": 13.6288, "lng": 79.4192, "suffix": "TIR"},
    {"city": "Kakinada", "lat": 16.9891, "lng": 82.2475, "suffix": "KKD"},
    {"city": "Guwahati", "lat": 26.1445, "lng": 91.7362, "suffix": "GAU"},
    {"city": "Imphal", "lat": 24.8170, "lng": 93.9368, "suffix": "IMF"},
    {"city": "Shillong", "lat": 25.5788, "lng": 91.8831, "suffix": "SHL"},
    {"city": "Aizawl", "lat": 23.7307, "lng": 92.7173, "suffix": "AJL"},
    {"city": "Kohima", "lat": 25.6751, "lng": 94.1086, "suffix": "KOH"},
    {"city": "Itanagar", "lat": 27.0844, "lng": 93.6053, "suffix": "HGO"},
    {"city": "Gangtok", "lat": 27.3314, "lng": 88.6138, "suffix": "GAY"},
    {"city": "Agartala", "lat": 23.8315, "lng": 91.2868, "suffix": "IXA"},
    {"city": "Chandigarh", "lat": 30.7333, "lng": 76.7794, "suffix": "IXC"},
    {"city": "Ludhiana", "lat": 30.9010, "lng": 75.8573, "suffix": "LDH"},
    {"city": "Amritsar", "lat": 31.6340, "lng": 74.8723, "suffix": "ATQ"},
    {"city": "Jalandhar", "lat": 31.3260, "lng": 75.5762, "suffix": "JUC"},
    {"city": "Shimla", "lat": 31.1048, "lng": 77.1734, "suffix": "SLV"},
    {"city": "Dharamshala", "lat": 32.2190, "lng": 76.3234, "suffix": "DHM"},
    {"city": "Dehradun", "lat": 30.3165, "lng": 78.0322, "suffix": "DED"},
    {"city": "Srinagar", "lat": 34.0837, "lng": 74.7973, "suffix": "SXR"},
    {"city": "Jammu", "lat": 32.7266, "lng": 74.8570, "suffix": "IXJ"},
    {"city": "Panaji", "lat": 15.4909, "lng": 73.8278, "suffix": "GOA"},
    {"city": "Raipur", "lat": 21.2514, "lng": 81.6296, "suffix": "RPR"},
    {"city": "Bilaspur", "lat": 22.0797, "lng": 82.1391, "suffix": "PND"},
    {"city": "Bhubaneswar", "lat": 20.2961, "lng": 85.8245, "suffix": "BBI"},
    {"city": "Panvel", "lat": 18.9894, "lng": 73.1175, "suffix": "PNV"},
    {"city": "Kalyan-Dombivli", "lat": 19.2403, "lng": 73.1305, "suffix": "KYN"},
    {"city": "Nashik", "lat": 19.9975, "lng": 73.7898, "suffix": "ISK"},
    {"city": "Solapur", "lat": 17.6599, "lng": 75.9064, "suffix": "SSE"},
    {"city": "Amravati", "lat": 20.9320, "lng": 77.7523, "suffix": "AMI"},
    {"city": "Aurangabad", "lat": 19.8762, "lng": 75.3433, "suffix": "IXU"},
    {"city": "Kolhapur", "lat": 16.7050, "lng": 74.2433, "suffix": "KLH"},
    {"city": "Belagavi", "lat": 15.8497, "lng": 74.4977, "suffix": "IXG"},
    {"city": "Hubbali-Dharwad", "lat": 15.3647, "lng": 75.1240, "suffix": "HBX"},
    {"city": "Mangaluru", "lat": 12.9141, "lng": 74.8560, "suffix": "IXE"},
    {"city": "Shivamogga", "lat": 13.9299, "lng": 75.5681, "suffix": "SMG"},
    {"city": "Tumakuru", "lat": 13.3392, "lng": 77.1140, "suffix": "TUM"},
    {"city": "Davanagere", "lat": 14.4644, "lng": 75.9218, "suffix": "DVG"},
    {"city": "Mysuru", "lat": 12.2958, "lng": 76.6394, "suffix": "MYS"},
    {"city": "Tirunelveli", "lat": 8.7139, "lng": 77.7567, "suffix": "TNV"},
    {"city": "Thanjavur", "lat": 10.7870, "lng": 79.1378, "suffix": "TJV"},
    {"city": "Thoothukudi", "lat": 8.7642, "lng": 78.1348, "suffix": "TCR"},
    {"city": "Erode", "lat": 11.3410, "lng": 77.7172, "suffix": "ERD"},
    {"city": "Tiruppur", "lat": 11.1085, "lng": 77.3411, "suffix": "TPR"},
    {"city": "Gandhinagar", "lat": 23.2156, "lng": 72.6369, "suffix": "GNR"},
    {"city": "Dahod", "lat": 22.8373, "lng": 74.2562, "suffix": "DHD"},
    {"city": "Silvassa", "lat": 20.2665, "lng": 73.0166, "suffix": "SLV"},
    {"city": "Daman", "lat": 20.3974, "lng": 72.8328, "suffix": "NMB"},
    {"city": "Sagar", "lat": 23.8388, "lng": 78.7378, "suffix": "SGR"},
    {"city": "Satna", "lat": 24.6005, "lng": 80.8322, "suffix": "STA"},
    {"city": "Karnal", "lat": 29.6857, "lng": 76.9905, "suffix": "KNL"},
    {"city": "Faridabad", "lat": 28.4089, "lng": 77.3178, "suffix": "FDB"},
    {"city": "Gurugram", "lat": 28.4595, "lng": 77.0266, "suffix": "GUG"}
]

BRAND_BASES = ["Domino's Pizza", "McDonald's", "Apna Sweets", "Tinku's", "Burger King", "Natural Ice Cream", "Haldiram's", "Bikanervala", "Faasos Wraps", "La Pino'z Pizza", "Subway", "KFC", "Punjab Dhaba", "Chai Point", "Baskin Robbins"]
CUISINES = ["North Indian, Fast Food", "South Indian", "Desserts, Ice Cream", "Bakery, Street Food", "Chinese, Mughlai"]
VEHICLES = ["Bicycle", "Scooter_Electric", "Motorbike", "Mini_Van"]

def populate_smart_cities_marketplace():
    total_cities = len(SMART_CITIES)
    total_restaurants = total_cities * RESTAURANTS_PER_CITY
    total_drivers = total_cities * DRIVERS_PER_CITY
    
    print(f"⚡ Establishing connection to '{DB_NAME}' for Smart Cities Deployment...")
    print(f"📊 Target Matrix: {total_cities} Cities | {total_restaurants} Restaurants | {total_drivers} Active Drivers")
    
    conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=True)
    cursor = conn.cursor()

    # Clean Tables Safely
    print("🧹 Scrubbing old database records clean...")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    
    tables_to_wipe = [
        "active_driver_telemetry", 
        "order_travel_logs",
        "merchant_partners_restaurants",
        "delivery_partners_drivers"
    ]
    
    for t in tables_to_wipe:
        try:
            cursor.execute(f"TRUNCATE TABLE {t};")
        except Exception as table_err:
            print(f"⚠️ Notice: Skipping truncation for {t} ({table_err})")
            
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

    print(f"🏢 Generating {total_restaurants} localized storefronts...")
    restaurant_insert_query = """
        INSERT INTO merchant_partners_restaurants (
            merchant_id, name, contact_details, spatial_point, cuisine_types, open_closed_hours, rating_avg, is_active
        ) VALUES (%s, %s, %s, ST_SRID(POINT(%s, %s), 4326), %s, '11:00 AM - 11:00 PM', %s, 1)
    """
    
    restaurant_records = []
    generated_restaurant_ids = []
    
    for zone in SMART_CITIES:
        for idx in range(RESTAURANTS_PER_CITY):
            r_id = f"RES-{zone['suffix']}-{idx+1:03d}"
            base_brand = random.choice(BRAND_BASES)
            r_name = f"{base_brand} ({zone['city']})"
            r_contact = f"+91999{random.randint(100000, 999999)}"
            cuisine = random.choice(CUISINES)
            rating = round(random.uniform(3.5, 4.9), 2)
            
            spread_lat = zone["lat"] + random.uniform(-0.03, 0.03)
            spread_lng = zone["lng"] + random.uniform(-0.03, 0.03)
            
            # Pass latitude and longitude as separate floats in that exact order to match ST_SRID(POINT(lat, lng))
            restaurant_records.append((r_id, r_name, r_contact, spread_lat, spread_lng, cuisine, rating))
            generated_restaurant_ids.append({"id": r_id, "lat": spread_lat, "lng": spread_lng, "city": zone["city"]})            
    cursor.executemany(restaurant_insert_query, restaurant_records)
    print(f"✅ SQL Ingestion Complete: {len(restaurant_records)} restaurants successfully deployed to disk.")

    # 2. Drivers Ingestion matching evaluated structure layout completely
    print(f"🏍️ Provisioning {total_drivers} fleet driver accounts inside MySQL matrix...")
    fleet_query = """
        INSERT INTO delivery_partners_drivers (
            driver_id, full_name, phone_number, vehicle_type, is_available, rating_avg, compliance_status
        ) VALUES (%s, %s, %s, %s, 1, %s, 'Approved')
    """
    
    drivers_list = []
    driver_telemetry_list = []
    driver_counter = 0
    for zone in SMART_CITIES:
        for _ in range(DRIVERS_PER_CITY):
            driver_counter += 1
            v_id = f"RIDER-{driver_counter:04d}"
            d_name = f"Rider Bot {driver_counter:04d}"
            d_phone = f"+91888{random.randint(100000, 999999)}"
            v_type = random.choice(VEHICLES)
            d_rating = round(random.uniform(4.0, 5.0), 2)
            
            drivers_list.append((v_id, d_name, d_phone, v_type, d_rating))
            
            # Generate initial telemetry position
            spread_lat = zone["lat"] + random.uniform(-0.02, 0.02)
            spread_lng = zone["lng"] + random.uniform(-0.02, 0.02)
            driver_telemetry_list.append((v_id, spread_lat, spread_lng))
            
    cursor.executemany(fleet_query, drivers_list)
    print(f"✅ Fleet Directory Set: Loaded {len(drivers_list)} active operators globally.")
    
    print("🛰️ Seeding initial active telemetry logs in MySQL...")
    telemetry_query = """
        INSERT INTO active_driver_telemetry (driver_id, current_gps_location, heading_degrees, last_ping_time)
        VALUES (%s, ST_SRID(POINT(%s, %s), 4326), 0.0, NOW())
    """
    cursor.executemany(telemetry_query, driver_telemetry_list)
    print(f"✅ Telemetry Seeded: Loaded {len(driver_telemetry_list)} starting driver positions.")
    
    cursor.close()
    conn.close()
    return generated_restaurant_ids, [d[0] for d in drivers_list]

def stream_live_telemetry_loop(hubs_pool, riders_pool):
    print(f"\n📡 Active Telemetry Core Status: Online. Running streaming loops for all {len(riders_pool)} drivers cross-nationally...")
    
    driver_anchors = {}
    for r_id in riders_pool:
        driver_anchors[r_id] = random.choice(hubs_pool)

    tick_count = 0
    while True:
        tick_count += 1
        start_time = time.time()
        
        pipeline = redis_client.pipeline()
        
        for r_id in riders_pool:
            anchor = driver_anchors[r_id]
            base_lat, base_lng = anchor["lat"], anchor["lng"]
            
            rider_index = int(r_id.split("-")[1])
            animation_time = time.time() * 0.015
            
            lat_step = 0.012 * math.sin(animation_time + rider_index) + random.uniform(-0.0001, 0.0001)
            lng_step = 0.012 * math.cos(animation_time + rider_index) + random.uniform(-0.0001, 0.0001)
            
            current_lat = base_lat + lat_step
            current_lng = base_lng + lng_step
            
            redis_key = f"rider:telemetry:{r_id}"
            pipeline.hset(redis_key, mapping={
                "latitude": str(current_lat),
                "longitude": str(current_lng),
                "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            
            pipeline.geoadd("driver_fleet_registry", (current_lng, current_lat, r_id))
            
        pipeline.execute()
        print(f"📡 Telemetry Frame #{tick_count} | Synced {len(riders_pool)} Smart City tracking vectors. Processing time: {time.time() - start_time:.3f}s")
        time.sleep(2.0)

if __name__ == "__main__":
    print("==========================================================================")
    print("🚀 NATIONWIDE 100 SMART CITIES AUTOMATED MARKETPLACE GENERATOR ENGINE")
    print("==========================================================================")
    
    hubs, riders = populate_smart_cities_marketplace()
    
    try:
        stream_live_telemetry_loop(hubs, riders)
    except KeyboardInterrupt:
        print("\n🛑 Telemetry transmission safely terminated. System data structures remain active.")
