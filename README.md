# BiteFlow: Hyperlocal Food Delivery & Geospatial Dispatch System

BiteFlow is a premium, end-to-end hyperlocal delivery network and routing cockpit designed for modern logistics. The platform integrates spatial database geometry, real-time vehicle telemetry, dynamic order batching, and three distinct responsive portals.

---

## 🚀 Key Architectural Features

- **Spatial SQL Matching**: Leverages coordinate mapping (`SRID 4326`) for geofencing and nearest-neighbor distance queries.
- **Dynamic Order Batching**: Automatically groups and stacks co-located orders onto single-rider routes to maximize efficiency.
- **Real-Time Telemetry**: Uses WebSockets to stream live GPS coordinates from delivery partners directly to dispatch maps.
- **Physical Road Routing**: Resolves exact road networks and ETAs.

---

## 🎨 System Portals

The application features three dedicated operations portals styled with a warm, professional culinary theme:

### 1. 🍳 Merchant Kitchen Portal
- **Live Orders Desk**: View active orders, transition cooking stages (`PREPARING` ➔ `READY`), and listen to dual-tone order notifications.
- **Auto-Assign Dispatch**: Directly triggers proximity algorithms to stack and match available fleet partners.
- **Catalog Manager**: Add new dishes, delete active menu items, and upload cover photos via a local file upload interface.
- **Live Coverage Map**: Track assigned drivers and delivery routes in real-time.

### 2. 🛵 Driver Delivery Portal
- **Mobile-First App Frame**: Sleek, high-contrast mobile dashboard optimized for outdoor use.
- **Live Map Navigation**: Displays active routes connecting the kitchen pickup location to the customer dropoff address.
- **Duty Controller**: Toggle online availability and stream live GPS simulator coordinates over WebSocket tunnels.

### 3. 👑 Platform Admin Cockpit
- **Operations Dashboard**: Monitor aggregated metrics (platform revenue, active fleet size, active merchants, and current checkouts).
- **Dynamic Pricing Controls**: Real-time surge multiplier slider updating checkout quotes instantly.
- **Compliance Controls**: Temporarily deactivate/activate merchants or suspend stores directly from the registry grid.
- **Central Dispatch Map**: Live Bangalore geospatial grid showing all active orders, delivery paths, dark stores, and driver markers.

---

## 🛠️ Technology Stack

- **Backend**: FastAPI (Python) & Uvicorn ASGI Server
- **Database**: MySQL (Spatial extensions for geography operations)
- **Caching & Locks**: Redis (Hot telemetry store & inventory reservation locks)
- **Frontend**: Responsive HTML5, Vanilla CSS, and JavaScript
- **Mapping**: Leaflet.js with CartoDB Voyager maps

---

## 🚀 Setup & Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/kumkum7080/Geospatial-logistics-router.git
   cd Geospatial-logistics-router
   ```

2. **Configure Environment**:
   Create a `.env` file containing database, Redis, and API details.

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize Database Schema**:
   ```bash
   python setup_db.py
   python data_seeder.py
   python seed_fleet.py
   ```

5. **Start the Application**:
   ```bash
   python main.py
   ```
   Access the portals:
   - **Consumer Discovery**: `http://localhost:8000/login`
   - **Merchant Portal**: `http://localhost:8000/merchant`
   - **Driver Portal**: `http://localhost:8000/driver`
   - **Admin Cockpit**: `http://localhost:8000/admin`
