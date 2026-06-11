import mysql.connector
from config import DB_CONFIG

db_config = DB_CONFIG

def run_spatial_matching(customer_lat, customer_lng):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        print(f"\n📍 Analyzing Geospatial Coordinates for Customer Location: ({customer_lat}, {customer_lng})")
        print("="*80)

        # 1. POLYGON MATCHING: Find which Dark Store covers this exact point
        customer_point_wkt = f"POINT({customer_lat} {customer_lng})"
        
        polygon_query = """
            SELECT hub_id, name, physical_address, rating_avg 
            FROM micro_fulfillment_centers_dark_stores
            WHERE ST_Contains(boundary_polygon, ST_GeomFromText(%s, 4326)) = 1;
        """
        
        cursor.execute(polygon_query, (customer_point_wkt,))
        matched_hubs = cursor.fetchall()
        
        print("\n🏢 [POLYGON SEARCH] Matched Operational Dark Store Hub:")
        if matched_hubs:
            for hub in matched_hubs:
                print(f"  ✅ Found: {hub['name']} ({hub['hub_id']})")
                print(f"     Address: {hub['physical_address']}")
        else:
            print("  ❌ No Dark Store Hub covers this location zone.")

        # 2. POINT NEAREST-NEIGHBOR SEARCH: Find and sort restaurants by real physical distance
        restaurant_query = """
            SELECT 
                merchant_id, 
                name, 
                cuisine_types,
                ST_Distance_Sphere(spatial_point, ST_GeomFromText(%s, 4326)) AS distance_meters
            FROM merchant_partners_restaurants
            WHERE is_active = 1
            ORDER BY distance_meters ASC
            LIMIT 5;
        """
        
        cursor.execute(restaurant_query, (customer_point_wkt,))
        nearest_restaurants = cursor.fetchall()
        
        print("\n🍳 [NEAREST-NEIGHBOR SEARCH] Closest Merchant Partner Restaurants:")
        if nearest_restaurants:
            for res in nearest_restaurants:
                distance_km = res['distance_meters'] / 1000.0
                print(f"  📍 {res['name']} ({res['merchant_id']})")
                print(f"     Cuisines: {res['cuisine_types']}")
                print(f"     As-The-Crow-Flies Distance: {distance_km:.2f} km away")
        else:
            print("  ❌ No active merchant restaurants found.")

    except mysql.connector.Error as err:
        print(f"❌ Spatial Query Failed: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    # Let's test a coordinate that drops directly inside our seeded Delhi Hub polygon!
    # Seeded Polygon bounding box was: 28.50 to 28.55 Lat, 77.20 to 77.25 Lng
    test_lat = 28.5250
    test_lng = 77.2200
    
    run_spatial_matching(test_lat, test_lng)
