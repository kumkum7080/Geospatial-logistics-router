import os
import sys
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass
import uuid
import json
import time
import asyncio
import urllib.request
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import FastAPI, HTTPException, status, WebSocket, WebSocketDisconnect, File, UploadFile
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import mysql.connector
from mysql.connector import pooling
import redis
from config import DB_CONFIG, REDIS_CONFIG

app = FastAPI(
    title="Geospatial Hyperlocal Routing Engine",
    description="Automated Geo-Fencing, Fleet Dispatch, Real-Road ETA Calculator, and Atomic Inventory Locks",
    version="1.0.0"
)

SURGE_MULTIPLIER_OVERRIDE = None

# --- 💾 TASK 1.1: CORE ARCHITECTURAL SHARED CONFIGURATION & CONNECTION POOL ---
db_config = DB_CONFIG

try:
    # Build thread-safe connection pooling to handle concurrent checkout spikes
    db_pool = pooling.MySQLConnectionPool(
        pool_name="hyperlocal_routing_pool",
        pool_size=10,  # Limits active persistent MySQL connections
        pool_reset_session=True,
        **db_config
    )
    print("⚡ [Initialization] Database Connection Pool established successfully with 10 slots.")
except Exception as e:
    print(f"❌ [Critical Error] Failed to initialize MySQL Connection Pool: {e}")
    db_pool = None

# Connect to Redis for Distributed Locks and State Caching
try:
    redis_client = redis.Redis(**REDIS_CONFIG)
except Exception as e:
    print(f"⚠️ Redis connection failed. Proceeding with mock lock simulation. Error: {e}")
    redis_client = None


# --- 📋 PYDANTIC SCHEMAS FOR INBOUND OPERATIONAL INGESTION ---
class OrderItem(BaseModel):
    item_id: str
    quantity: int = Field(..., gt=0)
    price_per_unit: float

class CheckoutPayload(BaseModel):
    customer_id: str
    latitude: float = Field(..., ge=-90.0, le=90.0)
    longitude: float = Field(..., ge=-180.0, le=180.0)
    items: List[OrderItem]
    delivery_address: str = ""


# --- 🛣️ EXTERNAL ROUTING NETWORK HELPER (OSRM ENGINE) ---
def get_road_network_metrics(rider_lat, rider_lng, rest_lat, rest_lng, cust_lat, cust_lng):
    """
    Queries OSRM server using physical street maps to calculate real driving paths.
    OSRM demands coordinates in: longitude,latitude format.
    """
    coords_string = f"{rider_lng},{rider_lat};{rest_lng},{rest_lat};{cust_lng},{cust_lat}"
    url = f"http://router.project-osrm.org/route/v1/driving/{coords_string}?overview=false"
    
    try:
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'GeospatialRoutingSystem/1.0 (Live Backend Integration)'}
        )
        with urllib.request.urlopen(req, timeout=1.5) as response:
            res_data = json.loads(response.read().decode())
            if res_data.get('routes'):
                legs = res_data['routes'][0]['legs']
                leg1_distance = legs[0]['distance']  # in meters
                leg1_duration = legs[0]['duration']  # in seconds
                leg2_distance = legs[1]['distance']  # in meters
                leg2_duration = legs[1]['duration']  # in seconds
                return leg1_distance, leg1_duration, leg2_distance, leg2_duration
    except Exception as e:
        print(f"⚠️ OSRM Engine Fallback Triggered: {e}")
        
    return 800.0, 180.0, 1500.0, 300.0


def get_multi_point_road_metrics(points: list) -> tuple:
    """
    Given a list of points (each a dict with 'lat' and 'lng'),
    computes the total distance (meters) and duration (seconds) of the path using OSRM.
    """
    coords = ";".join([f"{pt['lng']},{pt['lat']}" for pt in points])
    url = f"http://router.project-osrm.org/route/v1/driving/{coords}?overview=false"
    try:
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'GeospatialRoutingSystem/1.0 (Live Backend Integration)'}
        )
        with urllib.request.urlopen(req, timeout=1.5) as response:
            res_data = json.loads(response.read().decode())
            if res_data.get('routes'):
                route = res_data['routes'][0]
                return route['distance'], route['duration']
    except Exception as e:
        print(f"⚠️ OSRM Multi-Point Engine Fallback Triggered: {e}")
    
    # Simple straight-line fallback: sum of distances between successive points
    total_dist = 0.0
    for i in range(len(points) - 1):
        p1, p2 = points[i], points[i+1]
        import math
        lat1, lon1 = math.radians(p1['lat']), math.radians(p1['lng'])
        lat2, lon2 = math.radians(p2['lat']), math.radians(p2['lng'])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        dist = c * 6371000.0 # meters
        total_dist += dist
    
    # Speed assumption: 30 km/h (8.33 m/s)
    total_dur = total_dist / 8.33
    return total_dist, total_dur


# --- 🚀 CORE INTEGRATED TRANSACTION ENDPOINT WITH ATOMIC GUARDRAILS ---
@app.post("/checkout", status_code=status.HTTP_200_OK)
def process_spatial_checkout(payload: CheckoutPayload):
    parent_order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    sub_order_id = f"SUB-{uuid.uuid4().hex[:8].upper()}" # Primary tracking fallback id
    
    # 1. CONCURRENCY TIER: Claim In-Memory Inventory Locks via Redis
    lock_key = f"lock:inventory:{payload.customer_id}"
    redis_locked = False
    if redis_client:
        try:
            redis_locked = redis_client.set(lock_key, "LOCKED", ex=10, nx=True)
            if not redis_locked:
                raise HTTPException(status_code=423, detail="Transaction Locked: Double-submit prevention active.")
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            redis_locked = True 
    else:
        redis_locked = True

    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")

    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Start transactional lifecycle block
        conn.start_transaction()
        
        customer_wkt = f"POINT({payload.latitude} {payload.longitude})"
        
        # ----------------------------------------------------------------------
        # STEP 1: SPATIAL POLYGON GEO-FENCING
        # ----------------------------------------------------------------------
        hub_query = """
            SELECT hub_id, name FROM micro_fulfillment_centers_dark_stores
            WHERE ST_Contains(boundary_polygon, ST_GeomFromText(%s, 4326)) = 1 LIMIT 1;
        """
        cursor.execute(hub_query, (customer_wkt,))
        matched_hub = cursor.fetchone()
        if not matched_hub:
            matched_hub = {"hub_id": "DARKSTORE-DELHI-01", "name": "Delhi Core Fulfillment Hub"}
            
        # ----------------------------------------------------------------------
        # STEP 2: MERCHANT RESTAURANT SELECTION
        # ----------------------------------------------------------------------
        resolved_merchant_id = None
        for item in payload.items:
            cursor.execute("SELECT merchant_id FROM merchant_menus WHERE menu_item_id = %s LIMIT 1", (item.item_id,))
            menu_row = cursor.fetchone()
            if menu_row and menu_row['merchant_id']:
                resolved_merchant_id = menu_row['merchant_id']
                break

        matched_merchant = None
        if resolved_merchant_id:
            merchant_query = """
                SELECT merchant_id, name, ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng 
                FROM merchant_partners_restaurants
                WHERE merchant_id = %s LIMIT 1;
            """
            cursor.execute(merchant_query, (resolved_merchant_id,))
            matched_merchant = cursor.fetchone()

        if not matched_merchant:
            merchant_query = """
                SELECT merchant_id, name, ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng 
                FROM merchant_partners_restaurants
                WHERE is_active = 1
                ORDER BY ST_Distance_Sphere(spatial_point, ST_GeomFromText(%s, 4326)) ASC LIMIT 1;
            """
            cursor.execute(merchant_query, (customer_wkt,))
            matched_merchant = cursor.fetchone()

        if not matched_merchant:
            matched_merchant = {
                "merchant_id": "RESTAURANT-KITCHEN-01", 
                "name": "Central Kitchen Delhi 01",
                "lat": 28.6139,
                "lng": 77.2090
            }

        # ----------------------------------------------------------------------
        # TASK 1.3: AUTOMATED BASKET SPLITTING & ROW-LEVEL ATOMIC LOCKING
        # ----------------------------------------------------------------------
        base_subtotal = 0.0
        dark_store_items_list = []
        merchant_items_list = []
        
        for item in payload.items:
            # Query Catalog to check item context classification
            cursor.execute("SELECT product_id, price FROM inventory_catalog WHERE product_id = %s", (item.item_id,))
            catalog_row = cursor.fetchone()
            
            if catalog_row:
                # Grocery item found: Execute an immediate row-level lock (FOR UPDATE) to block race conditions
                cursor.execute(
                    """
                    SELECT quantity_on_hand FROM stock_levels 
                    WHERE dark_store_id = %s AND product_id = %s 
                    FOR UPDATE;
                    """,
                    (matched_hub['hub_id'], item.item_id)
                )
                stock_row = cursor.fetchone()
                
                if not stock_row or stock_row['quantity_on_hand'] < item.quantity:
                    raise HTTPException(
                        status_code=400, 
                        detail=f"Atomic Lock Failure: Product {item.item_id} out-of-stock or has insufficient units at {matched_hub['name']}."
                    )
                
                # Decrement physical stock safely within transaction scope
                cursor.execute(
                    "UPDATE stock_levels SET quantity_on_hand = quantity_on_hand - %s WHERE dark_store_id = %s AND product_id = %s",
                    (item.quantity, matched_hub['hub_id'], item.item_id)
                )
                
                # Record the transaction ledger lease inside cart_inventory_reservations
                expiry_stamp = datetime.now() + timedelta(minutes=15)
                cursor.execute(
                    """
                    INSERT INTO cart_inventory_reservations (customer_id, dark_store_id, product_id, quantity, expires_at)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (payload.customer_id, matched_hub['hub_id'], item.item_id, item.quantity, expiry_stamp)
                )
                
                resolved_price = float(catalog_row['price'] if catalog_row['price'] else item.price_per_unit)
                item_cost = resolved_price * item.quantity
                base_subtotal += item_cost
                
                dark_store_items_list.append({
                    "item_id": item.item_id, "quantity": item.quantity,
                    "unit_price": resolved_price, "total_price": item_cost
                })
            else:
                # Fallback item categorization: Treat as restaurant menu product
                cursor.execute("SELECT menu_item_id, price FROM merchant_menus WHERE menu_item_id = %s", (item.item_id,))
                menu_row = cursor.fetchone()
                
                resolved_price = float(menu_row['price'] if (menu_row and menu_row['price']) else item.price_per_unit)
                item_cost = resolved_price * item.quantity
                base_subtotal += item_cost
                
                merchant_items_list.append({
                    "item_id": item.item_id, "quantity": item.quantity,
                    "unit_price": resolved_price, "total_price": item_cost
                })

        # If database records are blank during early testing, map everything to merchant array to protect code testing
        if not dark_store_items_list and not merchant_items_list:
            for item in payload.items:
                item_cost = item.price_per_unit * item.quantity
                base_subtotal += item_cost
                merchant_items_list.append({
                    "item_id": item.item_id, "quantity": item.quantity,
                    "unit_price": item.price_per_unit, "total_price": item_cost
                })
        # STEP 3: LIVE ONLINE FLEET RIDER DISPATCH ASSIGNMENT (REDIS-ACCELERATED)
        # ----------------------------------------------------------------------
        restaurant_wkt = f"POINT({matched_merchant['lat']} {matched_merchant['lng']})"
        total_items_ordered = sum(item.quantity for item in payload.items)
        
        CAPACITY_MAP = {
            "Bicycle": 1,
            "Scooter_Electric": 2,
            "Motorbike": 2,
            "Mini_Van": 4
        }
        
        matched_rider = None
        is_batched_order = False
        batch_sequence_info = None
        
        # 3a. Search for busy drivers with remaining capacity for batching
        busy_drivers_query = """
            SELECT 
                t.driver_id, d.full_name as name, d.vehicle_type, d.rating_avg, t.current_load_count,
                ST_X(t.current_gps_location) as lat, ST_Y(t.current_gps_location) as lng
            FROM active_driver_telemetry t
            JOIN delivery_partners_drivers d ON t.driver_id = d.driver_id
            WHERE t.current_load_count > 0;
        """
        cursor.execute(busy_drivers_query)
        busy_drivers = cursor.fetchall()
        
        best_batch_rider = None
        min_detour_duration = 999999.0
        
        for driver in busy_drivers:
            vehicle_type = driver['vehicle_type']
            capacity = CAPACITY_MAP.get(vehicle_type, 2)
            if driver['current_load_count'] >= capacity:
                continue
            
            # Fetch active orders for this driver
            cursor.execute(
                """
                SELECT 
                    o.order_id, o.merchant_id, o.hub_id,
                    ST_X(op.final_point) as cust_lat, ST_Y(op.final_point) as cust_lng
                FROM orders o
                JOIN orders_sub_fulfillment osf ON o.order_id = osf.sub_order_id
                JOIN orders_parent op ON osf.order_id = op.order_id
                WHERE o.agent_id = %s AND o.order_status IN ('PLACED', 'PICKED_UP', 'ASSIGNED');
                """,
                (driver['driver_id'],)
            )
            active_orders = cursor.fetchall()
            if not active_orders:
                continue
            
            # Check source co-location (within 500m)
            first_order = active_orders[0]
            existing_src_lat = None
            existing_src_lng = None
            
            if first_order['merchant_id'] != 'N/A' and first_order['merchant_id']:
                cursor.execute("SELECT ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng FROM merchant_partners_restaurants WHERE merchant_id = %s", (first_order['merchant_id'],))
                res = cursor.fetchone()
                if res: existing_src_lat, existing_src_lng = res['lat'], res['lng']
            elif first_order['hub_id'] != 'N/A' and first_order['hub_id']:
                cursor.execute("SELECT ST_X(location) as lat, ST_Y(location) as lng FROM micro_fulfillment_centers_dark_stores WHERE hub_id = %s", (first_order['hub_id'],))
                res = cursor.fetchone()
                if res: existing_src_lat, existing_src_lng = res['lat'], res['lng']
            
            if existing_src_lat is None:
                continue
            
            cursor.execute(
                "SELECT ST_Distance_Sphere(ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326)) as dist;",
                (f"POINT({existing_src_lat} {existing_src_lng})", f"POINT({matched_merchant['lat']} {matched_merchant['lng']})")
            )
            source_dist = cursor.fetchone()['dist']
            if source_dist > 500.0:
                continue
            
            # Check destination proximity (within 3km)
            cursor.execute(
                "SELECT ST_Distance_Sphere(ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326)) as dist;",
                (f"POINT({first_order['cust_lat']} {first_order['cust_lng']})", customer_wkt)
            )
            dest_dist = cursor.fetchone()['dist']
            if dest_dist > 3000.0:
                continue
            
            # Compute detour paths
            pts = [
                {"lat": driver['lat'], "lng": driver['lng']},
                {"lat": existing_src_lat, "lng": existing_src_lng},
                {"lat": matched_merchant['lat'], "lng": matched_merchant['lng']}
            ]
            
            route1_pts = pts + [
                {"lat": first_order['cust_lat'], "lng": first_order['cust_lng']},
                {"lat": payload.latitude, "lng": payload.longitude}
            ]
            route2_pts = pts + [
                {"lat": payload.latitude, "lng": payload.longitude},
                {"lat": first_order['cust_lat'], "lng": first_order['cust_lng']}
            ]
            
            dist1, dur1 = get_multi_point_road_metrics(route1_pts)
            dist2, dur2 = get_multi_point_road_metrics(route2_pts)
            
            # Check SLAs (45 mins)
            sla_mins = 45.0
            kitchen_prep = 10.0
            
            _, dur_a1 = get_multi_point_road_metrics(route1_pts[:-1])
            eta_a1 = (dur_a1 / 60.0) + kitchen_prep
            eta_b1 = (dur1 / 60.0) + kitchen_prep
            
            _, dur_b2 = get_multi_point_road_metrics(route2_pts[:-1])
            eta_b2 = (dur_b2 / 60.0) + kitchen_prep
            eta_a2 = (dur2 / 60.0) + kitchen_prep
            
            r1_valid = eta_a1 <= sla_mins and eta_b1 <= sla_mins
            r2_valid = eta_a2 <= sla_mins and eta_b2 <= sla_mins
            
            if r1_valid or r2_valid:
                if r1_valid and r2_valid:
                    best_route = 1 if dur1 <= dur2 else 2
                elif r1_valid:
                    best_route = 1
                else:
                    best_route = 2
                
                best_dur = dur1 if best_route == 1 else dur2
                best_dist = dist1 if best_route == 1 else dist2
                eta_a = eta_a1 if best_route == 1 else eta_a2
                eta_b = eta_b1 if best_route == 1 else eta_b2
                
                if best_dur < min_detour_duration:
                    min_detour_duration = best_dur
                    best_batch_rider = {
                        "driver_id": driver['driver_id'],
                        "name": driver['name'],
                        "lat": driver['lat'],
                        "lng": driver['lng'],
                        "vehicle_type": driver['vehicle_type'],
                        "rating_avg": driver['rating_avg'],
                        "current_load_count": driver['current_load_count'],
                        "assigned_route": best_route,
                        "total_duration": best_dur,
                        "total_distance": best_dist,
                        "eta_a": eta_a,
                        "eta_b": eta_b,
                        "source_dist": source_dist,
                        "dest_dist": dest_dist
                    }
        
        if best_batch_rider:
            matched_rider = {
                "driver_id": best_batch_rider['driver_id'],
                "name": best_batch_rider['name'],
                "lat": best_batch_rider['lat'],
                "lng": best_batch_rider['lng'],
                "vehicle_type": best_batch_rider['vehicle_type'],
                "suitability_score": "BATCH_RIDER"
            }
            is_batched_order = True
            batch_sequence_info = {
                "batch_id": f"BATCH-{best_batch_rider['driver_id']}-{int(time.time())}",
                "route_sequence": ["Rider", "Source A", "Source B", "Customer A", "Customer B"] if best_batch_rider['assigned_route'] == 1 else ["Rider", "Source A", "Source B", "Customer B", "Customer A"],
                "detour_duration_mins": round(best_batch_rider['total_duration'] / 60.0, 1),
                "detour_distance_km": round(best_batch_rider['total_distance'] / 1000.0, 2),
                "eta_customer_a_mins": int(round(best_batch_rider['eta_a'])),
                "eta_customer_b_mins": int(round(best_batch_rider['eta_b']))
            }
            
            l1_dist = best_batch_rider['source_dist']
            l2_dist = best_batch_rider['dest_dist']
            total_road_distance_km = best_batch_rider['total_distance'] / 1000.0
            total_driving_minutes = best_batch_rider['total_duration'] / 60.0
            kitchen_prep_buffer = 10.0
            guaranteed_eta_minutes = int(round(best_batch_rider['eta_b']))
        
        else:
            # 3b. Fallback: Search for available riders (is_available = 1 AND current_load_count = 0)
            nearby_driver_ids = []
            if redis_client:
                try:
                    raw_drivers = redis_client.georadius(
                        "driver_fleet_registry", 
                        longitude=matched_merchant['lng'], latitude=matched_merchant['lat'], 
                        radius=10, unit="km"
                    )
                    nearby_driver_ids = [d.decode('utf-8') if isinstance(d, bytes) else d for d in raw_drivers]
                except Exception as e:
                    print(f"⚠️ Redis Geo Fetch Bypass: {e}")
            
            if nearby_driver_ids:
                id_placeholders = ",".join(["%s"] * len(nearby_driver_ids))
                rider_search_query = f"""
                    SELECT 
                        active_driver_telemetry.driver_id, delivery_partners_drivers.full_name, 
                        delivery_partners_drivers.vehicle_type, delivery_partners_drivers.rating_avg,
                        ST_X(active_driver_telemetry.current_gps_location) as lat, ST_Y(active_driver_telemetry.current_gps_location) as lng,
                        ST_Distance_Sphere(active_driver_telemetry.current_gps_location, ST_GeomFromText(%s, 4326)) AS distance_meters
                    FROM active_driver_telemetry
                    JOIN delivery_partners_drivers ON active_driver_telemetry.driver_id = delivery_partners_drivers.driver_id
                    WHERE delivery_partners_drivers.is_available = 1 AND active_driver_telemetry.current_load_count = 0 AND active_driver_telemetry.driver_id IN ({id_placeholders});
                """
                cursor.execute(rider_search_query, [restaurant_wkt] + nearby_driver_ids)
                available_riders = cursor.fetchall()
            else:
                rider_search_query = """
                    SELECT 
                        active_driver_telemetry.driver_id, delivery_partners_drivers.full_name, 
                        delivery_partners_drivers.vehicle_type, delivery_partners_drivers.rating_avg,
                        ST_X(active_driver_telemetry.current_gps_location) as lat, ST_Y(active_driver_telemetry.current_gps_location) as lng,
                        ST_Distance_Sphere(active_driver_telemetry.current_gps_location, ST_GeomFromText(%s, 4326)) AS distance_meters
                    FROM active_driver_telemetry
                    JOIN delivery_partners_drivers ON active_driver_telemetry.driver_id = delivery_partners_drivers.driver_id
                    WHERE delivery_partners_drivers.is_available = 1 AND active_driver_telemetry.current_load_count = 0
                      AND ST_Distance_Sphere(active_driver_telemetry.current_gps_location, ST_GeomFromText(%s, 4326)) <= 10000;
                """
                cursor.execute(rider_search_query, (restaurant_wkt, restaurant_wkt))
                available_riders = cursor.fetchall()

            if available_riders:
                best_score = -99999.0
                for rider in available_riders:
                    dist_km = rider['distance_meters'] / 1000.0
                    distance_score = max(0, 10.0 - dist_km) * 10
                    driver_rating = float(rider['rating_avg']) if rider['rating_avg'] else 5.0
                    reputation_score = (driver_rating / 5.0) * 100
                    vehicle_suitability_score = 100.0
                    if total_items_ordered > 5 and rider['vehicle_type'] == 'Bicycle':
                        vehicle_suitability_score = 30.0
                    
                    composite_suitability_score = ((distance_score * 0.50) + (reputation_score * 0.30) + (vehicle_suitability_score * 0.20))
                    if composite_suitability_score > best_score:
                        best_score = composite_suitability_score
                        matched_rider = {
                            "driver_id": rider['driver_id'], "name": rider['full_name'],
                            "lat": matched_merchant['lat'] + 0.001, "lng": matched_merchant['lng'] + 0.001,
                            "vehicle_type": rider['vehicle_type'],
                            "suitability_score": round(composite_suitability_score, 2)
                        }
            
            if not matched_rider:
                matched_rider = {"driver_id": "DRIVER-FALLBACK-99", "name": "Rider Delta (Smart Match Fallback)", "lat": 28.6145, "lng": 77.2095, "vehicle_type": "Motorbike", "suitability_score": "DEFAULT_RANK"}
            
            # STEP 4: PHYSICAL ROAD GRID ETA ANALYSIS via OSRM Engine
            l1_dist, l1_dur, l2_dist, l2_dur = get_road_network_metrics(
                rider_lat=matched_rider['lat'], rider_lng=matched_rider['lng'],
                rest_lat=matched_merchant['lat'], rest_lng=matched_merchant['lng'],
                cust_lat=payload.latitude, cust_lng=payload.longitude
            )
            total_road_distance_km = (l1_dist + l2_dist) / 1000.0
            total_driving_minutes = (l1_dur + l2_dur) / 60.0
            kitchen_prep_buffer = 10.0
            guaranteed_eta_minutes = int(round(total_driving_minutes + kitchen_prep_buffer))

        # Dynamic Surge Pricing Check via Redis Signals or Global Override
        surge_multiplier = 1.0
        surge_reason = "REGULAR_PRICING"
        if SURGE_MULTIPLIER_OVERRIDE is not None:
            surge_multiplier = SURGE_MULTIPLIER_OVERRIDE
            surge_reason = "ADMIN_SURGE_OVERRIDE_ACTIVE"
        else:
            if redis_client:
                try:
                    # Check for admin custom surge override first
                    custom_surge = redis_client.get("config:surge_multiplier")
                    if custom_surge:
                        surge_multiplier = float(custom_surge)
                        surge_reason = "ADMIN_SURGE_OVERRIDE_ACTIVE"
                    else:
                        active_orders_count = len(redis_client.keys("lock:inventory:*")) + 1
                        if active_orders_count >= 1:
                            surge_multiplier = 1.5
                            surge_reason = "HIGH_DEMAND_SURGE_ACTIVE"
                except: pass

        # ----------------------------------------------------------------------
        # TASK 1.2: MULTI-TABLE ACID ORDER INGESTION & SUB-FULFILLMENT ROUTING
        # ----------------------------------------------------------------------
        final_billable_amount = base_subtotal * surge_multiplier
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Update driver telemetry loads and availability in transaction
        if matched_rider['driver_id'] != "DRIVER-FALLBACK-99":
            if is_batched_order:
                cursor.execute(
                    "UPDATE active_driver_telemetry SET current_load_count = current_load_count + 1 WHERE driver_id = %s",
                    (matched_rider['driver_id'],)
                )
                new_load = best_batch_rider['current_load_count'] + 1
                cap = CAPACITY_MAP.get(matched_rider['vehicle_type'], 2)
                if new_load >= cap:
                    cursor.execute(
                        "UPDATE delivery_partners_drivers SET is_available = 0 WHERE driver_id = %s",
                        (matched_rider['driver_id'],)
                    )
            else:
                cursor.execute(
                    "UPDATE active_driver_telemetry SET current_load_count = 1 WHERE driver_id = %s",
                    (matched_rider['driver_id'],)
                )
                cap = CAPACITY_MAP.get(matched_rider['vehicle_type'], 2)
                if cap <= 1:
                    cursor.execute(
                        "UPDATE delivery_partners_drivers SET is_available = 0 WHERE driver_id = %s",
                        (matched_rider['driver_id'],)
                    )

        # 1. Insert Master Order record into orders_parent
        insert_order_query = """
            INSERT INTO orders_parent (order_id, customer_id, final_point, status_summary, total_transaction_amount, dynamic_surge_multiplier_id, delivery_address)
            VALUES (%s, %s, ST_GeomFromText(%s, 4326), 'Pending', %s, NULL, %s);
        """
        cursor.execute(insert_order_query, (parent_order_id, payload.customer_id, customer_wkt, final_billable_amount, payload.delivery_address))
        
        # 2a. Ingest Dark Store sub-order route branch if groceries are present
        if dark_store_items_list:
            ds_sub_id = f"SUB-DS-{uuid.uuid4().hex[:6].upper()}" if merchant_items_list else sub_order_id
            sub_order_id = ds_sub_id
            
            cursor.execute(
                "INSERT INTO orders_sub_fulfillment (sub_order_id, order_id, origin_dark_store_id, origin_merchant_id, current_workflow_status, scheduled_sla_minutes, delivery_fee) VALUES (%s, %s, %s, NULL, 'Pending', 30, 0.00);",
                (ds_sub_id, parent_order_id, matched_hub['hub_id'])
            )
            cursor.execute(
                "INSERT INTO orders (order_id, customer_id, hub_id, merchant_id, agent_id, total_amount, order_status) VALUES (%s, %s, %s, 'N/A', %s, %s, 'PLACED');",
                (ds_sub_id, payload.customer_id, matched_hub['hub_id'], matched_rider['driver_id'], sum(i['total_price'] for i in dark_store_items_list) * surge_multiplier)
            )
            for item in dark_store_items_list:
                cursor.execute(
                    "INSERT INTO order_items (order_id, sub_order_id, product_id, menu_item_id, quantity, unit_price, total_price) VALUES (%s, %s, %s, NULL, %s, %s, %s);",
                    (parent_order_id, ds_sub_id, item['item_id'], item['quantity'], item['unit_price'], item['total_price'])
                )

        # 2b. Ingest Restaurant merchant sub-order route branch if food items are present
        if merchant_items_list:
            me_sub_id = f"SUB-ME-{uuid.uuid4().hex[:6].upper()}" if dark_store_items_list else sub_order_id
            if not dark_store_items_list: sub_order_id = me_sub_id
            
            cursor.execute(
                "INSERT INTO orders_sub_fulfillment (sub_order_id, order_id, origin_dark_store_id, origin_merchant_id, current_workflow_status, scheduled_sla_minutes, delivery_fee) VALUES (%s, %s, NULL, %s, 'Pending', 45, 0.00);",
                (me_sub_id, parent_order_id, matched_merchant['merchant_id'])
            )
            cursor.execute(
                "INSERT INTO orders (order_id, customer_id, hub_id, merchant_id, agent_id, total_amount, order_status) VALUES (%s, %s, 'N/A', %s, %s, %s, 'PLACED');",
                (me_sub_id, payload.customer_id, matched_merchant['merchant_id'], matched_rider['driver_id'], sum(i['total_price'] for i in merchant_items_list) * surge_multiplier)
            )
            for item in merchant_items_list:
                cursor.execute(
                    "INSERT INTO order_items (order_id, sub_order_id, product_id, menu_item_id, quantity, unit_price, total_price) VALUES (%s, %s, NULL, %s, %s, %s, %s);",
                    (parent_order_id, me_sub_id, item['item_id'], item['quantity'], item['unit_price'], item['total_price'])
                )

        # Commit atomicity unit
        conn.commit()
        print(f"💾 [Database Pool Log] Transaction committed successfully for order sequence: {parent_order_id}")

        return {
            "status": "SUCCESS",
            "message": "Order processed through deep-spatial matching pipeline",
            "pricing_matrix": {
                "base_order_subtotal": round(base_subtotal, 2),
                "applied_surge_multiplier": surge_multiplier,
                "surge_tier_code": surge_reason,
                "final_calculated_total_cost": round(final_billable_amount, 2)
            },
            "transaction_ledger": {
                "parent_order_id": parent_order_id,
                "sub_order_id": sub_order_id,
                "redis_locks_active": redis_locked
            },
            "geospatial_dispatch_matrix": {
                "assigned_dark_store": f"{matched_hub['name']} ({matched_hub['hub_id']})",
                "assigned_merchant": f"{matched_merchant['name']} ({matched_merchant['merchant_id']})",
                "assigned_delivery_rider": f"{matched_rider['name']} ({matched_rider['driver_id']})",
                "assigned_driver_id": matched_rider["driver_id"],
                "rider_suitability_rank_score": matched_rider.get("suitability_score"),
                "is_batched_order": is_batched_order,
                "batch_sequence_info": batch_sequence_info
            },
            "physical_route_telemetry": {
                "rider_to_kitchen_road_distance_meters": round(l1_dist, 1),
                "kitchen_to_customer_road_distance_km": round(l2_dist / 1000.0, 2),
                "total_network_distance_km": round(total_road_distance_km, 2),
                "calculated_driving_time_mins": round(total_driving_minutes, 1),
                "kitchen_cooking_buffer_mins": kitchen_prep_buffer,
                "guaranteed_delivery_eta_mins": guaranteed_eta_minutes
            }
        }

    except mysql.connector.Error as err:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=f"Pool Database Transaction Failure: {err}")
    except HTTPException as he:
        if conn: conn.rollback()
        raise he
    finally:
        try: cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        except: pass
        if redis_client and redis_locked:
            try: redis_client.delete(lock_key)
            except: pass
        cursor.close()
        conn.close() # Connection cleanly sent back to the db_pool slots instead of shutting down


# --- 📍 CUSTOMER-FACING LIVE TELEMETRY FETCH ENGINE ---
@app.get("/api/order/track/{driver_id}")
def get_live_rider_location(driver_id: str):
    redis_key = f"rider:telemetry:{driver_id}"
    if not redis_client:
        raise HTTPException(status_code=503, detail="Telemetry Cache Offline: Redis cache layer is unavailable.")
        
    cached_data = redis_client.hgetall(redis_key)
    if not cached_data:
        raise HTTPException(status_code=404, detail=f"No active tracking stream found for driver {driver_id}. They might be offline.")
        
    return {
        "status": "TRACKING_ACTIVE",
        "driver_id": driver_id,
        "telemetry": {
            "latitude": float(cached_data.get("latitude")),
            "longitude": float(cached_data.get("longitude")),
            "last_updated": cached_data.get("last_updated")
        }
    }


@app.get("/api/order/details/{order_id}")
def get_order_details(order_id: str):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Get basic order info and sub-fulfillment
        query = """
            SELECT 
                o.order_id, o.customer_id, o.agent_id, o.order_status, o.total_amount,
                o.hub_id, o.merchant_id,
                ST_X(op.final_point) as cust_lat, ST_Y(op.final_point) as cust_lng,
                op.delivery_address, osf.scheduled_sla_minutes, op.created_at
            FROM orders o
            JOIN orders_sub_fulfillment osf ON o.order_id = osf.sub_order_id
            JOIN orders_parent op ON osf.order_id = op.order_id
            WHERE o.order_id = %s;
        """
        cursor.execute(query, (order_id,))
        order = cursor.fetchone()
        
        if not order:
            raise HTTPException(status_code=404, detail="Order not found.")
        
        # Check source coordinates
        src_lat = 28.5300
        src_lng = 77.2150
        src_name = "Source Kitchen"
        
        if order["merchant_id"] != "N/A" and order["merchant_id"]:
            cursor.execute("SELECT name, ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng FROM merchant_partners_restaurants WHERE merchant_id = %s", (order["merchant_id"],))
            res = cursor.fetchone()
            if res:
                src_lat = float(res["lat"])
                src_lng = float(res["lng"])
                src_name = res["name"]
        elif order["hub_id"] != "N/A" and order["hub_id"]:
            cursor.execute("SELECT name, ST_X(location) as lat, ST_Y(location) as lng FROM micro_fulfillment_centers_dark_stores WHERE hub_id = %s", (order["hub_id"],))
            res = cursor.fetchone()
            if res:
                src_lat = float(res["lat"])
                src_lng = float(res["lng"])
                src_name = res["name"]
                
        return {
            "order_id": order["order_id"],
            "status": order["order_status"],
            "agent_id": order["agent_id"],
            "total_amount": float(order["total_amount"]),
            "cust_lat": float(order["cust_lat"]) if order["cust_lat"] is not None else 0.0,
            "cust_lng": float(order["cust_lng"]) if order["cust_lng"] is not None else 0.0,
            "src_lat": src_lat,
            "src_lng": src_lng,
            "src_name": src_name,
            "delivery_address": order["delivery_address"] if order["delivery_address"] else "Not provided",
            "scheduled_sla_minutes": int(order["scheduled_sla_minutes"]) if order["scheduled_sla_minutes"] else 30,
            "created_at": order["created_at"].isoformat() if order["created_at"] else datetime.now().isoformat()
        }
    finally:
        cursor.close()
        conn.close()


# --- 🔄 WEBSOCKET CONNECTION ORCHESTRATOR FOR REAL-TIME FLEET TRACKING ---
@app.websocket("/ws/rider/{driver_id}")
async def websocket_rider_tracking_endpoint(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    print(f"🔌 [WebSocket Connected] Driver {driver_id} has opened a live telemetry tunnel.")
    
    try:
        while True:
            data = await websocket.receive_text()
            telemetry = json.loads(data)
            lat = telemetry.get("latitude")
            lng = telemetry.get("longitude")
            
            if lat is None or lng is None:
                await websocket.send_json({"status": "ERROR", "message": "Missing GPS telemetry keys."})
                continue
                
            redis_key = f"rider:telemetry:{driver_id}"
            timestamp_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            if redis_client:
                try:
                    redis_client.hset(redis_key, mapping={
                        "latitude": str(lat), "longitude": str(lng), "last_updated": timestamp_now
                    })
                    redis_client.expire(redis_key, 1800)
                    redis_client.geoadd("driver_fleet_registry", (float(lng), float(lat), driver_id))
                    redis_client.expire("driver_fleet_registry", 60)
                    
                    pubsub_payload = {
                        "driver_id": driver_id, "latitude": float(lat), "longitude": float(lng), "timestamp": timestamp_now
                    }
                    redis_client.publish(f"channel:track:{driver_id}", json.dumps(pubsub_payload))
                except Exception as cache_err:
                    print(f"⚠️ Redis Telemetry Cache/Spatial/PubSub Failure: {cache_err}")

            print(f"🛰️ [RIDER STREAM] Driver {driver_id} -> Live Coordinates: Lat {lat}, Lng {lng}")
            await websocket.send_json({
                "status": "TELEMETRY_ACK",
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "cached_in_redis": redis_client is not None
            })
            
    except WebSocketDisconnect:
        print(f"❌ [WebSocket Disconnected] Driver {driver_id} closed the tracking tunnel.")


# --- 👥 CUSTOMER-FACING LIVE WEBSOCKET TRACKING STREAM CHANNEL ---
@app.websocket("/ws/customer/track/{driver_id}")
async def websocket_customer_tracking_endpoint(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    print(f"👥 [Customer Track Connected] Client subscribed to live telemetry feed for driver {driver_id}")
    
    pubsub = None
    if redis_client:
        try:
            pubsub = redis_client.pubsub()
            pubsub.subscribe(f"channel:track:{driver_id}")
        except Exception as e:
            print(f"⚠️ Redis PubSub Subscription Failure: {e}")
            pubsub = None
        
    try:
        while True:
            if pubsub:
                try:
                    message = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                    if message and message['type'] == 'message':
                        telemetry_data = json.loads(message['data'])
                        await websocket.send_json({
                            "status": "LIVE_UPDATE",
                            "telemetry": telemetry_data
                        })
                except Exception as e:
                    print(f"⚠️ Error reading from Redis PubSub: {e}")
            await asyncio.sleep(0.1)

            
    except WebSocketDisconnect:
        print(f"🛑 [Customer Track Disconnected] Client dropped tracking channel for driver {driver_id}")
    finally:
        if pubsub:
            pubsub.unsubscribe(f"channel:track:{driver_id}")
            pubsub.close()


@app.post("/dispatch/optimize")
def optimize_dispatch_queues():
    """
    Scans the order queue for fallback or pending orders, clusters them by proximity,
    and dispatches them to optimal available fleet agents.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database Offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        conn.start_transaction()
        
        # 1. Fetch all active orders assigned to the fallback driver
        cursor.execute(
            """
            SELECT 
                o.order_id, o.merchant_id, o.hub_id, o.total_amount,
                ST_X(op.final_point) as cust_lat, ST_Y(op.final_point) as cust_lng
            FROM orders o
            JOIN orders_sub_fulfillment osf ON o.order_id = osf.sub_order_id
            JOIN orders_parent op ON osf.order_id = op.order_id
            WHERE o.agent_id = 'DRIVER-FALLBACK-99' AND o.order_status = 'PLACED';
            """
        )
        unassigned_orders = cursor.fetchall()
        if not unassigned_orders:
            return {"status": "SUCCESS", "message": "No unassigned/fallback orders in queue."}
        
        # 2. Fetch all available drivers
        cursor.execute(
            """
            SELECT 
                t.driver_id, d.full_name as name, d.vehicle_type, d.rating_avg, t.current_load_count,
                ST_X(t.current_gps_location) as lat, ST_Y(t.current_gps_location) as lng
            FROM active_driver_telemetry t
            JOIN delivery_partners_drivers d ON t.driver_id = d.driver_id
            WHERE d.is_available = 1 AND t.current_load_count = 0;
            """
        )
        available_drivers = cursor.fetchall()
        
        CAPACITY_MAP = {
            "Bicycle": 1,
            "Scooter_Electric": 2,
            "Motorbike": 2,
            "Mini_Van": 4
        }
        
        assignments = []
        
        # Simple greedy matching
        for order in unassigned_orders:
            if not available_drivers:
                break
            
            # Find source location
            src_lat = 28.6139
            src_lng = 77.2090
            if order['merchant_id'] != 'N/A' and order['merchant_id']:
                cursor.execute("SELECT ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng FROM merchant_partners_restaurants WHERE merchant_id = %s", (order['merchant_id'],))
                res = cursor.fetchone()
                if res: src_lat, src_lng = res['lat'], res['lng']
            elif order['hub_id'] != 'N/A' and order['hub_id']:
                cursor.execute("SELECT ST_X(location) as lat, ST_Y(location) as lng FROM micro_fulfillment_centers_dark_stores WHERE hub_id = %s", (order['hub_id'],))
                res = cursor.fetchone()
                if res: src_lat, src_lng = res['lat'], res['lng']
            
            # Find closest available driver
            best_driver = None
            min_dist = 9999999.0
            
            for d in available_drivers:
                cursor.execute(
                    "SELECT ST_Distance_Sphere(ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326)) as dist;",
                    (f"POINT({d['lat']} {d['lng']})", f"POINT({src_lat} {src_lng})")
                )
                dist = cursor.fetchone()['dist']
                if dist < min_dist:
                    min_dist = dist
                    best_driver = d
            
            if best_driver:
                # Assign order to this driver
                cursor.execute("UPDATE orders SET agent_id = %s WHERE order_id = %s", (best_driver['driver_id'], order['order_id']))
                cursor.execute("UPDATE active_driver_telemetry SET current_load_count = 1 WHERE driver_id = %s", (best_driver['driver_id'],))
                
                # Check capacity
                cap = CAPACITY_MAP.get(best_driver['vehicle_type'], 2)
                if cap <= 1:
                    cursor.execute("UPDATE delivery_partners_drivers SET is_available = 0 WHERE driver_id = %s", (best_driver['driver_id'],))
                    available_drivers.remove(best_driver)
                else:
                    best_driver['current_load_count'] = 1
                    available_drivers.remove(best_driver)
                
                assignments.append({
                    "order_id": order['order_id'],
                    "assigned_driver": best_driver['driver_id']
                })
                
        conn.commit()
        return {
            "status": "SUCCESS",
            "message": f"Processed unassigned queue. Dispatched {len(assignments)} orders.",
            "assignments": assignments
        }
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Bulk dispatch failure: {e}")
    finally:
        cursor.close()
        conn.close()


# --- 👥 CUSTOMER REGISTER & LOGIN API & PAGE ROUTERS ---

class RegisterPayload(BaseModel):
    full_name: str
    date_of_birth: str
    user_id: str
    password: str
    phone_number: str
    email: str = ""
    address: str = ""
    tier: str = "Standard"

class LoginPayload(BaseModel):
    user_id: str
    password: str

def slugify(text: str) -> str:
    import re
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s-]', '', text)
    text = re.sub(r'[\s-]+', '-', text)
    return text.strip('-')

@app.post("/api/register")
def register_customer(payload: RegisterPayload):
    import hashlib
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Check if user_id is already taken
        cursor.execute("SELECT customer_id FROM customers WHERE user_id = %s LIMIT 1", (payload.user_id,))
        existing = cursor.fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="This User ID is already taken. Please choose another.")
        
        # Check if phone number is already registered
        cursor.execute("SELECT customer_id FROM customers WHERE phone_number = %s LIMIT 1", (payload.phone_number,))
        existing_phone = cursor.fetchone()
        if existing_phone:
            raise HTTPException(status_code=400, detail="Phone number is already registered. Please log in.")
        
        customer_id = f"CUST-{uuid.uuid4().hex[:6].upper()}"
        password_hash = hashlib.sha256(payload.password.encode()).hexdigest()
        cursor.execute(
            """INSERT INTO customers 
               (customer_id, full_name, date_of_birth, user_id, password_hash, phone_number, email, address, tier) 
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (customer_id, payload.full_name, payload.date_of_birth, payload.user_id,
             password_hash, payload.phone_number, payload.email, payload.address, payload.tier)
        )
        conn.commit()
        return {
            "status": "SUCCESS",
            "customer_id": customer_id,
            "full_name": payload.full_name,
            "user_id": payload.user_id,
            "phone_number": payload.phone_number,
            "tier": payload.tier
        }
    except mysql.connector.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database Insertion Failure: {err}")
    finally:
        cursor.close()
        conn.close()

@app.post("/api/login")
def login_customer(payload: LoginPayload):
    import hashlib
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT customer_id, full_name, phone_number, tier, password_hash FROM customers WHERE user_id = %s LIMIT 1", (payload.user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="No account found with this User ID. Please register first.")
        
        password_hash = hashlib.sha256(payload.password.encode()).hexdigest()
        if user["password_hash"] != password_hash:
            raise HTTPException(status_code=401, detail="Incorrect password. Please try again.")
        
        return {
            "status": "SUCCESS",
            "customer_id": user["customer_id"],
            "full_name": user["full_name"],
            "phone_number": user["phone_number"],
            "tier": user["tier"]
        }
    finally:
        cursor.close()
        conn.close()

@app.get("/api/restaurants")
def get_restaurants():
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT merchant_id, name, contact_details, 
                   ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng, 
                   cuisine_types, open_closed_hours, rating_avg, image_url
            FROM merchant_partners_restaurants 
            WHERE is_active = 1
        """)
        restaurants = cursor.fetchall()
        
        # Fetch menu items for all active restaurants to map them
        cursor.execute("""
            SELECT merchant_id, item_name 
            FROM merchant_menus
        """)
        menu_rows = cursor.fetchall()
        
        # Group menu items by merchant ID
        merchant_items = {}
        for row in menu_rows:
            m_id = row['merchant_id']
            item_name = row['item_name'] if row['item_name'] is not None else ""
            if m_id not in merchant_items:
                merchant_items[m_id] = []
            merchant_items[m_id].append(item_name)
            
        for r in restaurants:
            r["slug"] = slugify(r["name"])
            # Format floats
            r["lat"] = float(r["lat"]) if r["lat"] is not None else 0.0
            r["lng"] = float(r["lng"]) if r["lng"] is not None else 0.0
            r["rating_avg"] = float(r["rating_avg"]) if r["rating_avg"] is not None else 0.0
            # Attach items list
            r["menu_items"] = ", ".join(merchant_items.get(r["merchant_id"], []))
            
        return restaurants
    finally:
        cursor.close()
        conn.close()

@app.get("/api/restaurants/{slug_or_id}/menu")
def get_restaurant_menu(slug_or_id: str):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Fetch all restaurants to match by id or slug
        cursor.execute("""
            SELECT merchant_id, name, contact_details, 
                   ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng, 
                   cuisine_types, open_closed_hours, rating_avg, image_url
            FROM merchant_partners_restaurants 
            WHERE is_active = 1
        """)
        restaurants = cursor.fetchall()
        matched_restaurant = None
        for r in restaurants:
            r["slug"] = slugify(r["name"])
            if r["merchant_id"] == slug_or_id or r["slug"] == slug_or_id:
                matched_restaurant = r
                break
        
        if not matched_restaurant:
            raise HTTPException(status_code=404, detail="Restaurant not found.")
        
        # Get menu items
        cursor.execute("""
            SELECT menu_item_id, item_name, price, image_url
            FROM merchant_menus 
            WHERE merchant_id = %s
        """, (matched_restaurant["merchant_id"],))
        menu_items = cursor.fetchall()
        for m in menu_items:
            m["price"] = float(m["price"])
            
        # Get dark store grocery items to offer as a complete supermarket catalog experience
        cursor.execute("""
            SELECT ic.product_id, ic.product_name, ic.price, sl.quantity_on_hand
            FROM inventory_catalog ic
            JOIN stock_levels sl ON ic.product_id = sl.product_id
            WHERE sl.quantity_on_hand > 0
        """)
        grocery_items = cursor.fetchall()
        for g in grocery_items:
            g["price"] = float(g["price"])
            
        # Convert lat/lng floats
        matched_restaurant["lat"] = float(matched_restaurant["lat"]) if matched_restaurant["lat"] is not None else 0.0
        matched_restaurant["lng"] = float(matched_restaurant["lng"]) if matched_restaurant["lng"] is not None else 0.0
        matched_restaurant["rating_avg"] = float(matched_restaurant["rating_avg"]) if matched_restaurant["rating_avg"] is not None else 0.0

        return {
            "restaurant": matched_restaurant,
            "menu": menu_items,
            "groceries": grocery_items
        }
    finally:
        cursor.close()
        conn.close()

# --- 🔌 KITCHEN CONTROL & MERCHANT PORTAL API SCHEMAS & ENDPOINTS ---

class AddDishPayload(BaseModel):
    item_name: str
    price: float

class DeleteDishPayload(BaseModel):
    menu_item_id: str

class UpdateStatusPayload(BaseModel):
    status: str

class MerchantRegisterPayload(BaseModel):
    user_id: str
    password: str
    full_name: str
    email: str
    phone_number: str
    merchant_id: Optional[str] = None
    restaurant_name: Optional[str] = None
    cuisines: Optional[str] = None

class MerchantLoginPayload(BaseModel):
    user_id: str
    password: str

@app.post("/api/merchant/register")
def register_merchant(payload: MerchantRegisterPayload):
    import hashlib
    import random
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Check duplicate user_id
        cursor.execute("SELECT user_id FROM merchant_users WHERE user_id = %s LIMIT 1", (payload.user_id,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Username is already taken. Please choose another.")
            
        # Check duplicate email
        cursor.execute("SELECT user_id FROM merchant_users WHERE email = %s LIMIT 1", (payload.email,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Email is already registered. Please login.")

        merchant_id = payload.merchant_id

        # If they want to create a new restaurant
        if payload.restaurant_name:
            merchant_id = f"REST-NEW-{uuid.uuid4().hex[:6].upper()}"
            # Random coordinates near Bangalore center (lat 12.9716, lng 77.5946)
            lat = 12.9716 + random.uniform(-0.025, 0.025)
            lng = 77.5946 + random.uniform(-0.025, 0.025)
            restaurant_wkt = f"POINT({lat} {lng})"
            
            cursor.execute(
                """INSERT INTO merchant_partners_restaurants 
                   (merchant_id, name, contact_details, spatial_point, cuisine_types, open_closed_hours, rating_avg, is_active)
                   VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s)""",
                (merchant_id, payload.restaurant_name, payload.phone_number, restaurant_wkt, 
                 payload.cuisines or "North Indian", "09:00-23:00", 4.0, True)
            )
            
            # Seed 4 default items for this new restaurant so it's not empty!
            default_dishes = [
                ("Special Butter Chicken", 280.0),
                ("Paneer Butter Masala", 240.0),
                ("Dal Makhani", 190.0),
                ("Garlic Naan", 60.0)
            ]
            for dish_name, price in default_dishes:
                item_id = f"MENU-{uuid.uuid4().hex[:8].upper()}"
                cursor.execute(
                    "INSERT INTO merchant_menus (menu_item_id, merchant_id, item_name, price) VALUES (%s, %s, %s, %s)",
                    (item_id, merchant_id, dish_name, price)
                )
        elif merchant_id:
            cursor.execute("SELECT merchant_id FROM merchant_partners_restaurants WHERE merchant_id = %s LIMIT 1", (merchant_id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Selected restaurant kitchen was not found in database.")
        else:
            raise HTTPException(status_code=400, detail="Must provide either restaurant_name or merchant_id to link.")

        # Hash password using sha256
        password_hash = hashlib.sha256(payload.password.encode()).hexdigest()
        
        cursor.execute(
            """INSERT INTO merchant_users 
               (user_id, password_hash, full_name, email, phone_number, merchant_id) 
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (payload.user_id, password_hash, payload.full_name, payload.email, payload.phone_number, merchant_id)
        )
        conn.commit()
        return {
            "status": "SUCCESS",
            "user_id": payload.user_id,
            "full_name": payload.full_name,
            "merchant_id": merchant_id
        }
    except mysql.connector.Error as err:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database Insertion Failure: {err}")
    finally:
        cursor.close()
        conn.close()

@app.post("/api/merchant/login")
def login_merchant(payload: MerchantLoginPayload):
    import hashlib
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Fetch user
        cursor.execute("SELECT user_id, password_hash, full_name, email, phone_number, merchant_id FROM merchant_users WHERE user_id = %s LIMIT 1", (payload.user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="No merchant account found with this Username.")
            
        password_hash = hashlib.sha256(payload.password.encode()).hexdigest()
        if user["password_hash"] != password_hash:
            raise HTTPException(status_code=401, detail="Incorrect password. Please try again.")
            
        # Get linked restaurant details
        merchant_id = user["merchant_id"]
        restaurant = None
        if merchant_id:
            cursor.execute(
                """SELECT name, cuisine_types, ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng, rating_avg, contact_details 
                   FROM merchant_partners_restaurants WHERE merchant_id = %s LIMIT 1""",
                (merchant_id,)
            )
            restaurant = cursor.fetchone()
            if restaurant:
                restaurant["lat"] = float(restaurant["lat"]) if restaurant["lat"] is not None else 0.0
                restaurant["lng"] = float(restaurant["lng"]) if restaurant["lng"] is not None else 0.0
                restaurant["rating_avg"] = float(restaurant["rating_avg"]) if restaurant["rating_avg"] is not None else 0.0
        
        return {
            "status": "SUCCESS",
            "user_id": user["user_id"],
            "full_name": user["full_name"],
            "merchant_id": merchant_id,
            "restaurant": restaurant
        }
    finally:
        cursor.close()
        conn.close()

@app.get("/api/merchant/restaurants")
def get_merchant_restaurants():
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT merchant_id, name, contact_details, 
                   ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng, 
                   cuisine_types, open_closed_hours, rating_avg 
            FROM merchant_partners_restaurants 
            WHERE is_active = 1
        """)
        restaurants = cursor.fetchall()
        for r in restaurants:
            r["lat"] = float(r["lat"]) if r["lat"] is not None else 0.0
            r["lng"] = float(r["lng"]) if r["lng"] is not None else 0.0
            r["rating_avg"] = float(r["rating_avg"]) if r["rating_avg"] is not None else 0.0
        return restaurants
    finally:
        cursor.close()
        conn.close()

@app.get("/api/merchant/{merchant_id}/dashboard")
def get_merchant_dashboard(merchant_id: str):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # 1. Get restaurant details
        cursor.execute("SELECT rating_avg, cuisine_types, image_url FROM merchant_partners_restaurants WHERE merchant_id = %s LIMIT 1", (merchant_id,))
        rest = cursor.fetchone()
        rating_avg = float(rest["rating_avg"]) if (rest and rest["rating_avg"] is not None) else 4.0
        cuisine_types = rest["cuisine_types"] if (rest and rest["cuisine_types"]) else "North Indian"
        image_url = rest["image_url"] if (rest and rest["image_url"]) else None

        # 2. Get active orders for this merchant (not Delivered or Cancelled)
        active_orders_query = """
            SELECT 
                o.order_id, o.customer_id, o.agent_id, o.order_status as status, o.total_amount,
                ST_X(op.final_point) as cust_lat, ST_Y(op.final_point) as cust_lng,
                op.delivery_address, osf.scheduled_sla_minutes, op.created_at,
                d.full_name as rider_name, d.phone_number as rider_phone, d.vehicle_type as rider_vehicle, d.rating_avg as rider_rating,
                ST_X(t.current_gps_location) as rider_lat, ST_Y(t.current_gps_location) as rider_lng
            FROM orders o
            JOIN orders_sub_fulfillment osf ON o.order_id = osf.sub_order_id
            JOIN orders_parent op ON osf.order_id = op.order_id
            LEFT JOIN delivery_partners_drivers d ON o.agent_id = d.driver_id
            LEFT JOIN active_driver_telemetry t ON o.agent_id = t.driver_id
            WHERE o.merchant_id = %s AND o.order_status NOT IN ('DELIVERED', 'CANCELLED')
            ORDER BY op.created_at DESC;
        """
        cursor.execute(active_orders_query, (merchant_id,))
        db_orders = cursor.fetchall()
        
        orders = []
        for o in db_orders:
            o_time = o["created_at"].strftime("%H:%M") if o["created_at"] else datetime.now().strftime("%H:%M")
            
            # Fetch items list
            cursor.execute("""
                SELECT menu_item_id as item_id, quantity, unit_price, total_price,
                       COALESCE((SELECT item_name FROM merchant_menus WHERE menu_item_id = order_items.menu_item_id LIMIT 1), 'Food Item') as item_name
                FROM order_items
                WHERE sub_order_id = %s
            """, (o["order_id"],))
            items = cursor.fetchall()
            for item in items:
                item["unit_price"] = float(item["unit_price"])
                item["total_price"] = float(item["total_price"])
            
            orders.append({
                "order_id": o["order_id"],
                "customer_id": o["customer_id"],
                "agent_id": o["agent_id"],
                "status": o["status"],
                "total_amount": float(o["total_amount"]),
                "cust_lat": float(o["cust_lat"]) if o["cust_lat"] is not None else None,
                "cust_lng": float(o["cust_lng"]) if o["cust_lng"] is not None else None,
                "delivery_address": o["delivery_address"],
                "scheduled_sla_minutes": o["scheduled_sla_minutes"],
                "created_time": o_time,
                "rider_name": o["rider_name"],
                "rider_phone": o["rider_phone"],
                "rider_vehicle": o["rider_vehicle"],
                "rider_rating": float(o["rider_rating"]) if o["rider_rating"] is not None else None,
                "rider_lat": float(o["rider_lat"]) if o["rider_lat"] is not None else None,
                "rider_lng": float(o["rider_lng"]) if o["rider_lng"] is not None else None,
                "items": items
            })
            
        # 3. Get menu catalog
        cursor.execute("SELECT menu_item_id, item_name, price, image_url FROM merchant_menus WHERE merchant_id = %s", (merchant_id,))
        menu_items = cursor.fetchall()
        for m in menu_items:
            m["price"] = float(m["price"])
            
        # 4. Stats: Revenue (Completed & preparing orders)
        cursor.execute("SELECT SUM(total_amount) as rev FROM orders WHERE merchant_id = %s AND order_status != 'CANCELLED'", (merchant_id,))
        rev_row = cursor.fetchone()
        total_revenue = float(rev_row['rev']) if (rev_row and rev_row['rev'] is not None) else 0.0
        
        # Top Selling Dishes
        top_dishes_query = """
            SELECT 
                m.item_name as name, 
                SUM(oi.quantity) as quantity_sold
            FROM order_items oi
            JOIN merchant_menus m ON oi.menu_item_id = m.menu_item_id
            JOIN orders o ON oi.sub_order_id = o.order_id
            WHERE o.merchant_id = %s
            GROUP BY oi.menu_item_id
            ORDER BY quantity_sold DESC
            LIMIT 5;
        """
        cursor.execute(top_dishes_query, (merchant_id,))
        top_dishes = cursor.fetchall()
        
        for d in top_dishes:
            d["quantity_sold"] = int(d["quantity_sold"])
            
        if not top_dishes:
            # Fallback mock items
            cursor.execute("SELECT item_name FROM merchant_menus WHERE merchant_id = %s LIMIT 3", (merchant_id,))
            menus = cursor.fetchall()
            mock_sales = [24, 18, 11]
            top_dishes = []
            for idx, m in enumerate(menus):
                top_dishes.append({
                    "name": m["item_name"],
                    "quantity_sold": mock_sales[idx] if idx < len(mock_sales) else 5
                })
                
        # 5. Analytics Cuisine Split
        cuisines_list = [c.strip().lower() for c in cuisine_types.split(',')] if cuisine_types else ["north indian"]
        cuisine_rev = {}
        if total_revenue > 0:
            for idx, c in enumerate(cuisines_list):
                if idx == len(cuisines_list) - 1:
                    cuisine_rev[c] = round(total_revenue - sum(cuisine_rev.values()), 2)
                else:
                    cuisine_rev[c] = round(total_revenue / len(cuisines_list), 2)
        else:
            mock_vals = [650.00, 480.00, 310.00]
            for idx, c in enumerate(cuisines_list):
                cuisine_rev[c] = mock_vals[idx] if idx < len(mock_vals) else 120.00
                
        return {
            "image_url": image_url,
            "cuisine_types": cuisine_types,
            "stats": {
                "total_revenue": total_revenue,
                "active_orders_count": len(orders),
                "rating_avg": rating_avg,
                "top_selling_dishes": top_dishes
            },
            "active_orders": orders,
            "menu_items": menu_items,
            "analytics": {
                "cuisine_revenue": cuisine_rev
            }
        }
    finally:
        cursor.close()
        conn.close()

@app.post("/api/merchant/{merchant_id}/menu/add")
def add_merchant_menu_item(merchant_id: str, payload: AddDishPayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        menu_item_id = f"MENU-{uuid.uuid4().hex[:8].upper()}"
        cursor.execute(
            "INSERT INTO merchant_menus (menu_item_id, merchant_id, item_name, price) VALUES (%s, %s, %s, %s)",
            (menu_item_id, merchant_id, payload.item_name, payload.price)
        )
        conn.commit()
        return {"status": "SUCCESS", "menu_item_id": menu_item_id}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/api/merchant/{merchant_id}/menu/delete")
def delete_merchant_menu_item(merchant_id: str, payload: DeleteDishPayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("DELETE FROM merchant_menus WHERE menu_item_id = %s AND merchant_id = %s", (payload.menu_item_id, merchant_id))
        conn.commit()
        return {"status": "SUCCESS"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/api/merchant/order/{sub_order_id}/update-status")
def update_merchant_order_status(sub_order_id: str, payload: UpdateStatusPayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        conn.start_transaction()
        cursor.execute("UPDATE orders SET order_status = %s WHERE order_id = %s", (payload.status, sub_order_id))
        cursor.execute("UPDATE orders_sub_fulfillment SET current_workflow_status = %s WHERE sub_order_id = %s", (payload.status, sub_order_id))
        
        cursor.execute("SELECT order_id FROM orders_sub_fulfillment WHERE sub_order_id = %s LIMIT 1", (sub_order_id,))
        parent_row = cursor.fetchone()
        if parent_row:
            parent_id = parent_row["order_id"]
            cursor.execute("UPDATE orders_parent SET status_summary = %s WHERE order_id = %s", (payload.status, parent_id))
            
        conn.commit()
        return {"status": "SUCCESS", "message": f"Order status updated to {payload.status}"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# --- 🏍️ DELIVERY DRIVER PORTAL SCHEMAS & ENDPOINTS ---

class DriverLoginPayload(BaseModel):
    driver_id: str
    phone_number: str

class DriverStatusUpdatePayload(BaseModel):
    status: str

class DriverAvailabilityPayload(BaseModel):
    is_available: bool

@app.post("/api/driver/login")
def login_driver(payload: DriverLoginPayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT driver_id, full_name, phone_number, vehicle_type, is_available, rating_avg FROM delivery_partners_drivers WHERE driver_id = %s LIMIT 1", (payload.driver_id,))
        driver = cursor.fetchone()
        if not driver:
            raise HTTPException(status_code=404, detail="Driver ID not found. Please contact fleet manager.")
            
        db_phone = driver["phone_number"].replace(" ", "").replace("-", "")
        payload_phone = payload.phone_number.replace(" ", "").replace("-", "")
        if db_phone not in payload_phone and payload_phone not in db_phone:
            raise HTTPException(status_code=401, detail="Phone number does not match fleet records.")
            
        return {
            "status": "SUCCESS",
            "driver_id": driver["driver_id"],
            "full_name": driver["full_name"],
            "vehicle_type": driver["vehicle_type"],
            "is_available": bool(driver["is_available"]),
            "rating_avg": float(driver["rating_avg"]) if driver["rating_avg"] is not None else 5.0
        }
    finally:
        cursor.close()
        conn.close()

@app.get("/api/driver/{driver_id}/active-task")
def get_driver_active_task(driver_id: str):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        query = """
            SELECT 
                o.order_id as sub_order_id, o.customer_id, o.merchant_id, o.hub_id, o.order_status, o.total_amount,
                ST_X(op.final_point) as cust_lat, ST_Y(op.final_point) as cust_lng,
                op.delivery_address, osf.scheduled_sla_minutes, op.created_at,
                r.name as rest_name, r.contact_details as rest_phone, r.cuisine_types as rest_cuisines,
                ST_X(r.spatial_point) as rest_lat, ST_Y(r.spatial_point) as lng_rest
            FROM orders o
            JOIN orders_sub_fulfillment osf ON o.order_id = osf.sub_order_id
            JOIN orders_parent op ON osf.order_id = op.order_id
            LEFT JOIN merchant_partners_restaurants r ON o.merchant_id = r.merchant_id
            WHERE o.agent_id = %s AND o.order_status NOT IN ('DELIVERED', 'CANCELLED')
            LIMIT 1;
        """
        cursor.execute(query, (driver_id,))
        order = cursor.fetchone()
        
        if not order:
            return {"status": "IDLE", "message": "No active tasks assigned."}
            
        o_time = order["created_at"].strftime("%Y-%m-%d %H:%M:%S") if order["created_at"] else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor.execute("""
            SELECT menu_item_id as item_id, quantity, unit_price, total_price,
                   COALESCE((SELECT item_name FROM merchant_menus WHERE menu_item_id = order_items.menu_item_id LIMIT 1), 'Food Item') as item_name
            FROM order_items
            WHERE sub_order_id = %s
        """, (order["sub_order_id"],))
        items = cursor.fetchall()
        for item in items:
            item["unit_price"] = float(item["unit_price"])
            item["total_price"] = float(item["total_price"])
            
        return {
            "status": "ACTIVE",
            "order": {
                "sub_order_id": order["sub_order_id"],
                "customer_id": order["customer_id"],
                "merchant_id": order["merchant_id"],
                "hub_id": order["hub_id"],
                "order_status": order["order_status"],
                "total_amount": float(order["total_amount"]),
                "cust_lat": float(order["cust_lat"]) if order["cust_lat"] is not None else None,
                "cust_lng": float(order["cust_lng"]) if order["cust_lng"] is not None else None,
                "delivery_address": order["delivery_address"],
                "scheduled_sla_minutes": order["scheduled_sla_minutes"],
                "created_time": o_time,
                "rest_name": order["rest_name"],
                "rest_phone": order["rest_phone"],
                "rest_cuisines": order["rest_cuisines"],
                "rest_lat": float(order["rest_lat"]) if order["rest_lat"] is not None else None,
                "rest_lng": float(order["lng_rest"]) if order["lng_rest"] is not None else None,
                "items": items
            }
        }
    finally:
        cursor.close()
        conn.close()

@app.post("/api/driver/order/{order_id}/status")
def update_driver_order_status(order_id: str, payload: DriverStatusUpdatePayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        conn.start_transaction()
        
        cursor.execute("SELECT agent_id, order_status FROM orders WHERE order_id = %s LIMIT 1", (order_id,))
        order = cursor.fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found.")
            
        driver_id = order["agent_id"]
        
        cursor.execute("UPDATE orders SET order_status = %s WHERE order_id = %s", (payload.status, order_id))
        cursor.execute("UPDATE orders_sub_fulfillment SET current_workflow_status = %s WHERE sub_order_id = %s", (payload.status, order_id))
        
        cursor.execute("SELECT order_id FROM orders_sub_fulfillment WHERE sub_order_id = %s LIMIT 1", (order_id,))
        parent_row = cursor.fetchone()
        if parent_row:
            parent_id = parent_row["order_id"]
            cursor.execute("UPDATE orders_parent SET status_summary = %s WHERE order_id = %s", (payload.status, parent_id))
            
        if payload.status == "DELIVERED":
            cursor.execute("UPDATE active_driver_telemetry SET current_load_count = 0 WHERE driver_id = %s", (driver_id,))
            cursor.execute("UPDATE delivery_partners_drivers SET is_available = 1 WHERE driver_id = %s", (driver_id,))
            
            if parent_row:
                cursor.execute("SELECT ST_X(final_point) as lat, ST_Y(final_point) as lng FROM orders_parent WHERE order_id = %s LIMIT 1", (parent_id,))
                coords = cursor.fetchone()
                if coords:
                    c_lat = float(coords["lat"]) if coords["lat"] is not None else 12.9716
                    c_lng = float(coords["lng"]) if coords["lng"] is not None else 77.5946
                    cursor.execute(
                        """INSERT INTO historical_delivery_ledger (order_id, vehicle_id, final_lat, final_lon, status)
                           VALUES (%s, %s, %s, %s, 'DELIVERED')
                           ON DUPLICATE KEY UPDATE status = 'DELIVERED', updated_at = CURRENT_TIMESTAMP""",
                        (order_id, driver_id, c_lat, c_lng)
                    )

        conn.commit()
        return {"status": "SUCCESS", "message": f"Order status successfully updated to {payload.status}"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/api/driver/{driver_id}/availability")
def update_driver_availability(driver_id: str, payload: DriverAvailabilityPayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        val = 1 if payload.is_available else 0
        cursor.execute("UPDATE delivery_partners_drivers SET is_available = %s WHERE driver_id = %s", (val, driver_id))
        conn.commit()
        return {"status": "SUCCESS", "is_available": payload.is_available}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# --- 👑 NETWORK OWNER (ADMIN) CONTROL SCHEMAS & ENDPOINTS ---

class AdminSurgePayload(BaseModel):
    surge_multiplier: float

class AdminMerchantTogglePayload(BaseModel):
    is_active: bool

@app.get("/api/admin/metrics")
def get_admin_metrics():
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT SUM(total_transaction_amount) as rev FROM orders_parent WHERE status_summary != 'CANCELLED'")
        row = cursor.fetchone()
        total_rev = float(row['rev']) if (row and row['rev'] is not None) else 0.0
        
        cursor.execute("SELECT COUNT(*) as active_cnt FROM orders WHERE order_status NOT IN ('DELIVERED', 'CANCELLED')")
        active_cnt = cursor.fetchone()['active_cnt']
        
        cursor.execute("SELECT COUNT(*) as online_cnt FROM delivery_partners_drivers WHERE is_available = 1")
        online_cnt = cursor.fetchone()['online_cnt']
        
        cursor.execute("SELECT COUNT(*) as total_drivers FROM delivery_partners_drivers")
        total_drivers = cursor.fetchone()['total_drivers']
        
        cursor.execute("SELECT COUNT(*) as merchant_cnt FROM merchant_partners_restaurants WHERE is_active = 1")
        merchant_cnt = cursor.fetchone()['merchant_cnt']
        
        surge = 1.0
        if SURGE_MULTIPLIER_OVERRIDE is not None:
            surge = SURGE_MULTIPLIER_OVERRIDE
        elif redis_client:
            try:
                custom_surge = redis_client.get("config:surge_multiplier")
                if custom_surge:
                    surge = float(custom_surge)
            except: pass
            
        return {
            "status": "SUCCESS",
            "metrics": {
                "total_revenue": total_rev,
                "active_orders_count": active_cnt,
                "online_drivers_count": online_cnt,
                "total_drivers_count": total_drivers,
                "active_merchants_count": merchant_cnt,
                "surge_multiplier": surge
            }
        }
    finally:
        cursor.close()
        conn.close()

@app.get("/api/admin/operations-data")
def get_admin_operations_data():
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("""
            SELECT 
                d.driver_id, d.full_name, d.phone_number, d.vehicle_type, d.is_available, d.rating_avg,
                t.current_load_count, ST_X(t.current_gps_location) as lat, ST_Y(t.current_gps_location) as lng
            FROM delivery_partners_drivers d
            LEFT JOIN active_driver_telemetry t ON d.driver_id = t.driver_id
        """)
        drivers = cursor.fetchall()
        for d in drivers:
            d["lat"] = float(d["lat"]) if d["lat"] is not None else None
            d["lng"] = float(d["lng"]) if d["lng"] is not None else None
            d["rating_avg"] = float(d["rating_avg"]) if d["rating_avg"] is not None else 5.0
            d["is_available"] = bool(d["is_available"])
            
        cursor.execute("""
            SELECT 
                merchant_id, name, contact_details, ST_X(spatial_point) as lat, ST_Y(spatial_point) as lng,
                cuisine_types, open_closed_hours, rating_avg, is_active
            FROM merchant_partners_restaurants
        """)
        restaurants = cursor.fetchall()
        for r in restaurants:
            r["lat"] = float(r["lat"]) if r["lat"] is not None else 0.0
            r["lng"] = float(r["lng"]) if r["lng"] is not None else 0.0
            r["rating_avg"] = float(r["rating_avg"]) if r["rating_avg"] is not None else 4.0
            r["is_active"] = bool(r["is_active"])
            
        cursor.execute("""
            SELECT 
                o.order_id as sub_order_id, o.customer_id, o.merchant_id, o.agent_id, o.order_status, o.total_amount,
                ST_X(op.final_point) as cust_lat, ST_Y(op.final_point) as cust_lng,
                r.name as rest_name, ST_X(r.spatial_point) as rest_lat, ST_Y(r.spatial_point) as rest_lng
            FROM orders o
            JOIN orders_sub_fulfillment osf ON o.order_id = osf.sub_order_id
            JOIN orders_parent op ON osf.order_id = op.order_id
            LEFT JOIN merchant_partners_restaurants r ON o.merchant_id = r.merchant_id
            WHERE o.order_status NOT IN ('DELIVERED', 'CANCELLED')
        """)
        active_orders = cursor.fetchall()
        for o in active_orders:
            o["total_amount"] = float(o["total_amount"])
            o["cust_lat"] = float(o["cust_lat"]) if o["cust_lat"] is not None else None
            o["cust_lng"] = float(o["cust_lng"]) if o["cust_lng"] is not None else None
            o["rest_lat"] = float(o["rest_lat"]) if o["rest_lat"] is not None else None
            o["rest_lng"] = float(o["rest_lng"]) if o["rest_lng"] is not None else None
            
        cursor.execute("""
            SELECT hub_id, name, physical_address, ST_X(location) as lat, ST_Y(location) as lng, ST_AsText(boundary_polygon) as polygon_wkt
            FROM micro_fulfillment_centers_dark_stores
        """)
        dark_stores = cursor.fetchall()
        for ds in dark_stores:
            ds["lat"] = float(ds["lat"]) if ds["lat"] is not None else 0.0
            ds["lng"] = float(ds["lng"]) if ds["lng"] is not None else 0.0
            
        return {
            "status": "SUCCESS",
            "drivers": drivers,
            "restaurants": restaurants,
            "active_orders": active_orders,
            "dark_stores": dark_stores
        }
    finally:
        cursor.close()
        conn.close()

@app.post("/api/admin/surge")
def update_admin_surge(payload: AdminSurgePayload):
    global SURGE_MULTIPLIER_OVERRIDE
    SURGE_MULTIPLIER_OVERRIDE = payload.surge_multiplier
    
    redis_success = False
    if redis_client:
        try:
            redis_client.set("config:surge_multiplier", str(payload.surge_multiplier))
            redis_success = True
        except Exception as e:
            print(f"⚠️ Failed to write surge multiplier to Redis: {e}")
            
    return {
        "status": "SUCCESS", 
        "message": f"Global surge multiplier set to {payload.surge_multiplier}x (Redis synced: {redis_success})"
    }

@app.post("/api/admin/restaurant/{merchant_id}/toggle-active")
def toggle_restaurant_active(merchant_id: str, payload: AdminMerchantTogglePayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        val = 1 if payload.is_active else 0
        cursor.execute("UPDATE merchant_partners_restaurants SET is_active = %s WHERE merchant_id = %s", (val, merchant_id))
        conn.commit()
        return {"status": "SUCCESS", "merchant_id": merchant_id, "is_active": payload.is_active}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/api/admin/dispatch-optimize")
def trigger_admin_dispatch_optimize():
    return optimize_dispatch_queues()

class RestaurantPhotoPayload(BaseModel):
    image_url: str

class MenuItemPhotoPayload(BaseModel):
    image_url: str

@app.post("/api/upload")
def upload_file(file: UploadFile = File(...)):
    try:
        os.makedirs("static/uploads", exist_ok=True)
        file_ext = os.path.splitext(file.filename)[1]
        unique_name = f"{uuid.uuid4().hex}{file_ext}"
        target_path = os.path.join("static/uploads", unique_name)
        with open(target_path, "wb") as buffer:
            buffer.write(file.file.read())
        return {"status": "SUCCESS", "url": f"/static/uploads/{unique_name}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/merchant/{merchant_id}/update-photo")
def update_merchant_restaurant_photo(merchant_id: str, payload: RestaurantPhotoPayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "UPDATE merchant_partners_restaurants SET image_url = %s WHERE merchant_id = %s",
            (payload.image_url, merchant_id)
        )
        conn.commit()
        return {"status": "SUCCESS", "merchant_id": merchant_id, "image_url": payload.image_url}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.post("/api/merchant/{merchant_id}/menu/{menu_item_id}/update-photo")
def update_merchant_menu_item_photo(merchant_id: str, menu_item_id: str, payload: MenuItemPhotoPayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database Connection Pool is offline.")
    conn = db_pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            "UPDATE merchant_menus SET image_url = %s WHERE menu_item_id = %s AND merchant_id = %s",
            (payload.image_url, menu_item_id, merchant_id)
        )
        conn.commit()
        return {"status": "SUCCESS", "menu_item_id": menu_item_id, "image_url": payload.image_url}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# --- Page Routers Serving Static Files ---

@app.get("/admin", response_class=HTMLResponse)
def admin_login_view():
    return FileResponse("static/admin/login.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/admin/dashboard", response_class=HTMLResponse)
def admin_dashboard_view():
    return FileResponse("static/admin/dashboard.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/driver", response_class=HTMLResponse)
def driver_login_view():
    return FileResponse("static/driver/login.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/driver/dashboard", response_class=HTMLResponse)
def driver_dashboard_view():
    return FileResponse("static/driver/dashboard.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/merchant", response_class=HTMLResponse)
def merchant_login_view():
    return FileResponse("static/merchant/login.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/merchant/register", response_class=HTMLResponse)
def merchant_register_view():
    return FileResponse("static/merchant/register.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/merchant/dashboard", response_class=HTMLResponse)
def merchant_dashboard_view():
    return FileResponse("static/merchant/dashboard.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})


@app.get("/")
def root_redirect():
    return RedirectResponse(url="/login")

@app.get("/login", response_class=HTMLResponse)
def login_view():
    return FileResponse("static/consumer/login.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/register", response_class=HTMLResponse)
def register_view():
    return FileResponse("static/consumer/register.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/delhi", response_class=HTMLResponse)
def consumer_landing_page():
    return FileResponse("static/consumer/index.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/delhi/{restaurant_slug}", response_class=HTMLResponse)
def consumer_restaurant_page(restaurant_slug: str):
    return FileResponse("static/consumer/restaurant.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/bangalore", response_class=HTMLResponse)
def consumer_bangalore_landing_page():
    return FileResponse("static/consumer/index.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/bangalore/{restaurant_slug}", response_class=HTMLResponse)
def consumer_bangalore_restaurant_page(restaurant_slug: str):
    return FileResponse("static/consumer/restaurant.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/checkout", response_class=HTMLResponse)
def consumer_checkout_page():
    return FileResponse("static/consumer/cart.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/checkout/address", response_class=HTMLResponse)
def checkout_address_view():
    return FileResponse("static/consumer/address.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

@app.get("/order-status/{order_id}", response_class=HTMLResponse)
def consumer_tracking_page(order_id: str):
    return FileResponse("static/consumer/track.html", headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

# Mount static files folder
app.mount("/static", StaticFiles(directory="static"), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
