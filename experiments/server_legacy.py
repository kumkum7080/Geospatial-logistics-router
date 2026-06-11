import uuid
import json
import time
from typing import List, Dict
from datetime import datetime
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import redis
import mysql.connector
from mysql.connector import pooling
from config import DB_CONFIG, REDIS_CONFIG

app = FastAPI(title="Legacy Food & Quick-Commerce Engine Experiment")

# =========================================================================
# 🔌 CONNECTION POOL INITIALIZATION (PRODUCTION SAFE)
# =========================================================================
try:
    # Tier 1: Hot Cache (Redis Storage)
    r = redis.Redis(**REDIS_CONFIG)
    
    # Tier 2: Warm Ledger Pool (Ensures thread safety across concurrent connections)
    db_config = {
        **DB_CONFIG,
        "pool_name": "logistics_pool",
        "pool_size": 10
    }
    db_pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)
    print("🚀 Securely connected to Tier 1 (Redis) and Tier 2 (MySQL Connection Pool)!")
except Exception as e:
    print(f"❌ Critical Infrastructure Connection Error: {e}")

# =========================================================================
# 📝 DATA MANAGEMENT SCHEMAS
# =========================================================================
class OrderItem(BaseModel):
    product_id: str = None    
    menu_item_id: str = None  
    quantity: int
    unit_price: float

class OrderRequest(BaseModel):
    customer_id: str
    latitude: float
    longitude: float
    items: List[OrderItem]
    total_amount: float
    dark_store_id: str = None
    merchant_id: str = None

# Tracks live WebSocket customer links for broadcasting telemetry maps
class StreamingBroadcastManager:
    def __init__(self):
        self.active_listeners: Dict[str, List[WebSocket]] = {}

    async def subscribe_customer(self, rider_id: str, websocket: WebSocket):
        await websocket.accept()
        if rider_id not in self.active_listeners:
            self.active_listeners[rider_id] = []
        self.active_listeners[rider_id].append(websocket)

    def unsubscribe_customer(self, rider_id: str, websocket: WebSocket):
        if rider_id in self.active_listeners:
            self.active_listeners[rider_id].remove(websocket)

    async def broadcast_gps(self, rider_id: str, message: dict):
        if rider_id in self.active_listeners:
            for socket in self.active_listeners[rider_id]:
                try:
                    await socket.send_json(message)
                except Exception:
                    pass # Dropped connections are cleaned out organically on lifecycle events

broadcaster = StreamingBroadcastManager()

# =========================================================================
# 🛒 ENDPOINT: INSTANT CHECKOUT & STOCK LOCKING (test_checkout.py Link)
# =========================================================================
@app.post("/api/v1/checkout")
async def process_checkout(order_data: OrderRequest):
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    sub_order_id = f"SUB-{uuid.uuid4().hex[:8].upper()}"
    
    # 🔴 TIER 1: HOT LAYER - Redis Transient Stock Reservation Locking
    if order_data.dark_store_id:
        for item in order_data.items:
            if item.product_id:
                lock_key = f"lock:{order_data.dark_store_id}:{item.product_id}"
                r.setex(lock_key, 300, item.quantity)
                print(f"🔒 Tier 1 Lock: Reserved {item.quantity} units of {item.product_id} in Redis.")

    # 🟡 TIER 2: WARM LAYER - Thread-Safe Structural MySQL Commits
    try:
        conn = db_pool.get_connection()
        with conn.cursor(dictionary=True) as cursor:
            # 1. Insert Parent Order using standard SRID 4326 mapping conventions (POINT expects Longitude then Latitude)
            parent_sql = """
                INSERT INTO orders_parent (order_id, customer_id, final_point, total_transaction_amount, status_summary)
                VALUES (%s, %s, ST_SRID(POINT(%s, %s), 4326), %s, 'Pending')
            """
            cursor.execute(parent_sql, (order_id, order_data.customer_id, order_data.longitude, order_data.latitude, order_data.total_amount))
            
            # 2. Fragment transaction parameters down to specialized sub-fulfillment tables
            sub_sql = """
                INSERT INTO orders_sub_fulfillment (sub_order_id, order_id, origin_dark_store_id, origin_merchant_id, current_workflow_status)
                VALUES (%s, %s, %s, %s, 'Pending')
            """
            cursor.execute(sub_sql, (sub_order_id, order_id, order_data.dark_store_id, order_data.merchant_id))
            
            conn.commit()
            print(f"📝 Tier 2 Commit: Order {order_id} split into Sub-Order {sub_order_id} via MySQL Pool.")
            
    except Exception as err:
        if 'conn' in locals() and conn.is_connected():
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database transaction aborted: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close() # Return connection link safely back to pool allocation maps

    return {
        "status": "SUCCESS",
        "message": "Order processed cleanly and warm records committed.",
        "parent_order_id": order_id,
        "sub_order_id": sub_order_id,
        "redis_locks_active": True
    }

# =========================================================================
# 🏍️ TUNNEL 1: HIGH-FREQUENCY RIDER POSITION INGESTION (test_tracking.py)
# =========================================================================
@app.websocket("/ws/rider/{driver_id}")
async def telemetry_rider_tunnel(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    print(f"🏍️ [Rider Link Opened] Telemetry tracking activated for: {driver_id}")
    try:
        while True:
            data = await websocket.receive_text()
            coords = json.loads(data)
            
            lat = coords.get("latitude")
            lon = coords.get("longitude")
            
            # Update high-speed localized geofence matrices via Redis GEO engine strings
            r.geoadd("active_driver_locations", (lon, lat, driver_id))
            
            # Broadcast updates to any active consumer map listeners tracking this device
            payload = {
                "rider_id": driver_id,
                "latitude": lat,
                "longitude": lon,
                "updated_at": datetime.now().strftime("%H:%M:%S")
            }
            await broadcaster.broadcast_gps(driver_id, payload)
            
            # Return acknowledgement back up to rider device stack
            await websocket.send_json({"status": "ACK", "captured": time.time()})
    except WebSocketDisconnect:
        print(f"🏍️ [Rider Link Closed] Stream disconnected from: {driver_id}")

# =========================================================================
# 👥 TUNNEL 2: LIVE LIVE CUSTOMER UI TELEMETRY MAP FEED (test_realtime_tracking.py)
# =========================================================================
@app.websocket("/ws/customer/track/{rider_id}")
async def customer_tracking_tunnel(websocket: WebSocket, rider_id: str):
    await broadcaster.subscribe_customer(rider_id, websocket)
    try:
        while True:
            await websocket.receive_text() # Keep channel open awaiting outbound broadcast events
    except WebSocketDisconnect:
        broadcaster.unsubscribe_customer(rider_id, websocket)

# =========================================================================
# 🚀 INITIALIZATION LOOP BLOCK
# =========================================================================
if __name__ == "__main__":
    import uvicorn
    # Legacy experiment only. The real backend entrypoint is main.py.
    uvicorn.run("experiments.server_legacy:app", host="127.0.0.1", port=8080, reload=True)
