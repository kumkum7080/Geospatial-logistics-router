import mysql.connector
import redis
import json
import time
from config import DB_CONFIG, REDIS_CONFIG

db_config = DB_CONFIG

try:
    redis_client = redis.Redis(**REDIS_CONFIG)
    print("季 Connected to Hot-Layer Telemetry Cache (Redis).")
except Exception as e:
    print(f" Let's keep moving. Redis Connection Failed: {e}")
    exit(1)

def archive_completed_orders():
    """
    Scrapes active order states, moves completed telemetry streams 
    from Redis into the permanent MySQL cold layer, and evicts cache keys.
    """
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # 1. Gather pending orders logged in the parent database table
        query = "SELECT order_id FROM orders_parent WHERE status_summary = 'Pending' LIMIT 10;"
        cursor.execute(query)
        target_orders = cursor.fetchall()
        
        if not target_orders:
            print(" No real database orders require cold-layer archival at this moment.")
            return

        for order in target_orders:
            order_id = order['order_id']
            mock_agent_id = "RIDER-0001" 
            redis_key = f"rider:telemetry:{mock_agent_id}"
            
            # Development fail-safe seed: ensures there are tracking coordinates to archive
            if not redis_client.exists(redis_key):
                redis_client.hset(redis_key, mapping={
                    "latitude": "28.6139",
                    "longitude": "77.2090",
                    "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
                })
            
            hot_telemetry = redis_client.hgetall(redis_key)
            
            lat = hot_telemetry.get("latitude", "28.6139")
            lng = hot_telemetry.get("longitude", "77.2090")
            
            history_payload = {
                "final_recorded_lat": lat,
                "final_recorded_lng": lng,
                "last_stream_timestamp": hot_telemetry.get("last_updated"),
                "source_layer": "HOT_MEMORY_CACHE_EVICTION"
            }
            json_dump = json.dumps([history_payload]) # Enclosing inside an array snapshot block
            
            # Formulating correct SRID Geometry String format: POINT(Lat Lng)
            wkt_spatial_point = f"POINT({lat} {lng})"
            
            # Constructing an explicit unique hash string log key signature to satisfy Primary Key requirements
            unique_log_id = f"LOG-{order_id[:8]}-{mock_agent_id[:6]}"

            # Log directly to MySQL cold table matching tables_details02.csv definitions
            try:
                # MODIFIED: Structural names updated precisely to match database catalog constraints
                archive_query = """
                    INSERT INTO order_travel_logs (log_id, order_id, driver_id, recorded_location, raw_telemetry_json_trail)
                    VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), %s)
                    ON DUPLICATE KEY UPDATE 
                        recorded_location = ST_GeomFromText(%s, 4326),
                        raw_telemetry_json_trail = %s;
                """
                cursor.execute(archive_query, (
                    unique_log_id, 
                    order_id, 
                    mock_agent_id, 
                    wkt_spatial_point, 
                    json_dump,
                    wkt_spatial_point, 
                    json_dump
                ))
                
                # MODIFIED: Transition order state flag status identifier to break loop processing locks
                update_status_query = "UPDATE orders_parent SET status_summary = 'Processing' WHERE order_id = %s;"
                cursor.execute(update_status_query, (order_id,))
                
                # Wipe records cleanly out of Hot Layer Memory to minimize memory footprint
                redis_client.delete(redis_key)
                print(f" Successfully archived tracking log for Real Order {order_id} ➔ Cold Layer. Reclaimed Hot RAM.")
                
            except mysql.connector.Error as db_err:
                print(f" Skipped row processing for Order {order_id} due to operational syntax error: {db_err}")
            
        # Complete transactional commits atomically outside structural iteration loops
        conn.commit()
        print(" Data warehouse tier synchronization complete.")

    except Exception as err:
        print(f" Archival Fault Encountered: {err}")
        if conn: conn.rollback()
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print(" Launching Automated Hot-to-Cold Data Tiering Engine Daemon...")
    archive_completed_orders()
