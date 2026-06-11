import pymysql
from config import DB_CONFIG

# --- DATABASE CONFIGURATION ---
DB_HOST = DB_CONFIG["host"]
DB_PORT = DB_CONFIG["port"]
DB_USER = DB_CONFIG["user"]
DB_PASSWORD = DB_CONFIG["password"]
DB_NAME = DB_CONFIG["database"]

def build_advanced_schema():
    try:
        # Connect to MySQL Server
        connection = pymysql.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            user=DB_USER,
            password=DB_PASSWORD,
            autocommit=True
        )
        cursor = connection.cursor()
        
        # Fresh Database Recreation
        cursor.execute(f"DROP DATABASE IF EXISTS `{DB_NAME}`;")
        cursor.execute(f"CREATE DATABASE `{DB_NAME}`;")
        cursor.execute(f"USE `{DB_NAME}`;")
        print(f"🚀 Initialized Hyperlocal Food Delivery Database: '{DB_NAME}'")
        
    except Exception as e:
        print(f"❌ Initial Connection Failure: {e}")
        return

    tables = {}
    
    # 1. Customers
    tables['customers'] = """
    CREATE TABLE customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        full_name VARCHAR(160) NOT NULL,
        phone_number VARCHAR(20),
        tier ENUM('Standard', 'Business', 'Premium_VIP') DEFAULT 'Standard',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # 2. Micro-Fulfillment Centers (Dark Stores)
    tables['micro_fulfillment_centers_dark_stores'] = """
    CREATE TABLE micro_fulfillment_centers_dark_stores (
        hub_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(160) NOT NULL,
        physical_address VARCHAR(255),
        open_closed_hours VARCHAR(50),
        rating_avg DECIMAL(3,2),
        location POINT NOT NULL SRID 4326,
        boundary_polygon POLYGON NOT NULL SRID 4326,
        manager_id VARCHAR(50),
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        SPATIAL INDEX(location),
        SPATIAL INDEX(boundary_polygon)
    );
    """
    
    # 3. Merchant Partners & Restaurants
    tables['merchant_partners_restaurants'] = """
    CREATE TABLE merchant_partners_restaurants (
        merchant_id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(160) NOT NULL,
        contact_details VARCHAR(50),
        spatial_point POINT NOT NULL SRID 4326,
        cuisine_types VARCHAR(255),
        open_closed_hours VARCHAR(50),
        rating_avg DECIMAL(3,2),
        is_active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        SPATIAL INDEX(spatial_point)
    );
    """

    # 4. Inventory Catalog
    tables['inventory_catalog'] = """
    CREATE TABLE inventory_catalog (
        product_id VARCHAR(50) PRIMARY KEY,
        provider_id VARCHAR(50) NOT NULL,
        product_name VARCHAR(255) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        provider_type ENUM('MFC', 'Merchant') NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    """

    # 5. Stock Levels
    tables['stock_levels'] = """
    CREATE TABLE stock_levels (
        dark_store_id VARCHAR(50) NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        quantity_on_hand INT NOT NULL DEFAULT 0,
        PRIMARY KEY (dark_store_id, product_id),
        FOREIGN KEY (dark_store_id) REFERENCES micro_fulfillment_centers_dark_stores(hub_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES inventory_catalog(product_id) ON DELETE CASCADE
    );
    """

    # 6. Cart Inventory Reservations
    tables['cart_inventory_reservations'] = """
    CREATE TABLE cart_inventory_reservations (
        customer_id VARCHAR(50) NOT NULL,
        dark_store_id VARCHAR(50) NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        quantity INT NOT NULL,
        expires_at TIMESTAMP NOT NULL,
        PRIMARY KEY (customer_id, dark_store_id, product_id),
        FOREIGN KEY (dark_store_id) REFERENCES micro_fulfillment_centers_dark_stores(hub_id) ON DELETE CASCADE,
        FOREIGN KEY (product_id) REFERENCES inventory_catalog(product_id) ON DELETE CASCADE
    );
    """

    # 7. Merchant Menus
    tables['merchant_menus'] = """
    CREATE TABLE merchant_menus (
        menu_item_id VARCHAR(50) PRIMARY KEY,
        merchant_id VARCHAR(50) NOT NULL,
        item_name VARCHAR(255) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        FOREIGN KEY (merchant_id) REFERENCES merchant_partners_restaurants(merchant_id) ON DELETE CASCADE
    );
    """

    # 8. Delivery Partners & Drivers
    tables['delivery_partners_drivers'] = """
    CREATE TABLE delivery_partners_drivers (
        driver_id VARCHAR(50) PRIMARY KEY,
        full_name VARCHAR(160) NOT NULL,
        phone_number VARCHAR(20),
        vehicle_type ENUM('Bicycle', 'Scooter_Electric', 'Motorbike', 'Mini_Van') NOT NULL,
        is_available BOOLEAN DEFAULT TRUE,
        rating_avg DECIMAL(3,2),
        compliance_status VARCHAR(50) DEFAULT 'APPROVED'
    );
    """

    # 9. Driver Telemetry (High-Frequency)
    tables['active_driver_telemetry'] = """
    CREATE TABLE active_driver_telemetry (
        driver_id VARCHAR(50) PRIMARY KEY,
        current_gps_location POINT NOT NULL SRID 4326,
        heading_degrees DECIMAL(5,2) DEFAULT 0.0,
        current_load_count INT DEFAULT 0,
        last_ping_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (driver_id) REFERENCES delivery_partners_drivers(driver_id) ON DELETE CASCADE,
        SPATIAL INDEX(current_gps_location)
    );
    """

    # 10. Parent Orders (Customer Level)
    tables['orders_parent'] = """
    CREATE TABLE orders_parent (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50) NOT NULL,
        final_point POINT NOT NULL SRID 4326,
        status_summary VARCHAR(50) DEFAULT 'Pending',
        total_transaction_amount DECIMAL(10,2) NOT NULL,
        dynamic_surge_multiplier_id VARCHAR(50) DEFAULT NULL,
        delivery_address VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        SPATIAL INDEX(final_point)
    );
    """

    # 11. Sub-Fulfillment Logic (Per Source Fulfillment)
    tables['orders_sub_fulfillment'] = """
    CREATE TABLE orders_sub_fulfillment (
        sub_order_id VARCHAR(50) PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        origin_dark_store_id VARCHAR(50) DEFAULT NULL,
        origin_merchant_id VARCHAR(50) DEFAULT NULL,
        current_workflow_status VARCHAR(50) DEFAULT 'Pending',
        scheduled_sla_minutes INT DEFAULT 30,
        delivery_fee DECIMAL(10,2) DEFAULT 0.00,
        FOREIGN KEY (order_id) REFERENCES orders_parent(order_id) ON DELETE CASCADE
    );
    """

    # 12. General Orders
    tables['orders'] = """
    CREATE TABLE orders (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50) NOT NULL,
        hub_id VARCHAR(50) NOT NULL,
        merchant_id VARCHAR(50) NOT NULL,
        agent_id VARCHAR(50) NOT NULL,
        total_amount DECIMAL(10,2) NOT NULL,
        order_status VARCHAR(50) DEFAULT 'PLACED'
    );
    """

    # 13. Order Items
    tables['order_items'] = """
    CREATE TABLE order_items (
        item_row_id INT AUTO_INCREMENT PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        sub_order_id VARCHAR(50) NOT NULL,
        product_id VARCHAR(50) DEFAULT NULL,
        menu_item_id VARCHAR(50) DEFAULT NULL,
        quantity INT NOT NULL,
        unit_price DECIMAL(10,2) NOT NULL,
        total_price DECIMAL(10,2) NOT NULL
    );
    """

    # 14. Order Travel Logs
    tables['order_travel_logs'] = """
    CREATE TABLE order_travel_logs (
        log_id VARCHAR(100) PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        driver_id VARCHAR(50) NOT NULL,
        recorded_location POINT NOT NULL SRID 4326,
        raw_telemetry_json_trail JSON,
        SPATIAL INDEX(recorded_location)
    );
    """

    # 15. Historical Delivery Ledger
    tables['historical_delivery_ledger'] = """
    CREATE TABLE historical_delivery_ledger (
        order_id VARCHAR(50) PRIMARY KEY,
        vehicle_id VARCHAR(50) NOT NULL,
        final_lat DECIMAL(10,8) NOT NULL,
        final_lon DECIMAL(11,8) NOT NULL,
        status VARCHAR(20) DEFAULT 'DELIVERED',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );
    """

    # 16. Merchant Users (Authentication & Restaurant Link)
    tables['merchant_users'] = """
    CREATE TABLE merchant_users (
        user_id VARCHAR(50) PRIMARY KEY,
        password_hash VARCHAR(64) NOT NULL,
        full_name VARCHAR(150) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        phone_number VARCHAR(20) NOT NULL,
        merchant_id VARCHAR(50) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (merchant_id) REFERENCES merchant_partners_restaurants(merchant_id) ON DELETE SET NULL
    );
    """

    execution_order = [
        'customers',
        'micro_fulfillment_centers_dark_stores',
        'merchant_partners_restaurants',
        'inventory_catalog',
        'stock_levels',
        'cart_inventory_reservations',
        'merchant_menus',
        'merchant_users',
        'delivery_partners_drivers',
        'active_driver_telemetry',
        'orders_parent',
        'orders_sub_fulfillment',
        'orders',
        'order_items',
        'order_travel_logs',
        'historical_delivery_ledger'
    ]

    # Loop through tables and execute creation strings sequentially
    for table_name in execution_order:
        try:
            cursor.execute(tables[table_name])
            print(f"✅ Created Table: {table_name}")
        except Exception as e:
            print(f"❌ Failed to create table '{table_name}': {e}")
            cursor.close()
            connection.close()
            return

    print("\n🎉 Schema Deployment Complete. All Zomato-level tables unified!")
    cursor.close()
    connection.close()

if __name__ == "__main__":
    build_advanced_schema()