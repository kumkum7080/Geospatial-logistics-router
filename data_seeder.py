import mysql.connector
from datetime import datetime
from config import DB_CONFIG

# Exact verified database configuration
db_config = DB_CONFIG


def seed_database():
    try:
        print("⚡ Connecting to Spatial Database for a clean ground-up asset seeding...")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 1. DEACTIVATE FOREIGN KEYS TO WIPE CORES CLEAN WITHOUT CONFLICTS
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        print("🧹 Scrubbing old operational records clean...")
        tables_to_clean = [
            "cart_inventory_reservations",
            "stock_levels",
            "inventory_catalog",
            "merchant_menus",
            "orders_sub_fulfillment", 
            "orders_parent", 
            "orders",
            "order_items",
            "order_travel_logs",
            "micro_fulfillment_centers_dark_stores", 
            "merchant_partners_restaurants", 
            "customers"
        ]
        for table in tables_to_clean:
            cursor.execute(f"TRUNCATE TABLE {table};")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        # 2. SEED CUSTOMERS TABLE
        print("👥 Seeding Operational Customers...")
        customer_query = """
            INSERT INTO customers (customer_id, full_name, phone_number, tier, created_at)
            VALUES (%s, %s, %s, %s, %s)
        """
        customers_data = [
            ("CUST-0001", "Test Pilot Alpha", "+1234567890", "Standard", datetime.now()),
            ("CUST-0002", "Logistics Enterprise B", "+1987654321", "Business", datetime.now()),
            ("CUST-0003", "Premium VIP User", "+1555444333", "Premium_VIP", datetime.now())
        ]
        cursor.executemany(customer_query, customers_data)

        # 3. SEED DARK STORES TABLE (With SRID 4326 explicitly enforced)
        print("🏢 Seeding Core Logistic Hubs (Dark Stores)...")
        hub_query = """
            INSERT INTO micro_fulfillment_centers_dark_stores (
                hub_id, name, physical_address, created_at, open_closed_hours, rating_avg, boundary_polygon, location, manager_id
            ) VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), ST_GeomFromText(%s, 4326), %s)
        """
        # Valid bounding box polygon covering a section of Delhi (Latitude Longitude order)
        delhi_polygon_wkt = "POLYGON((28.50 77.20, 28.55 77.20, 28.55 77.25, 28.50 77.25, 28.50 77.20))"
        delhi_hub_location_wkt = "POINT(28.5250 77.2200)"
        
        hubs_data = [
            (
                "DARKSTORE-DELHI-01",              # 1. hub_id
                "Delhi Core Fulfillment Hub",       # 2. name
                "123 Main Logistics Lane, Delhi",  # 3. physical_address
                datetime.now(),                    # 4. created_at
                "06:00-23:00",                     # 5. open_closed_hours
                4.8,                               # 6. rating_avg
                delhi_polygon_wkt,                 # 7. boundary_polygon
                delhi_hub_location_wkt,            # 8. location
                "MGR-001"                          # 9. manager_id
            )
        ]
        cursor.executemany(hub_query, hubs_data)

        # 4. SEED MERCHANTS TABLE (With SRID 4326 explicitly enforced)
        print("🍳 Seeding Merchant Partner Restaurants...")
        merchant_query = """
            INSERT INTO merchant_partners_restaurants (
                merchant_id, name, contact_details, spatial_point, cuisine_types, open_closed_hours, rating_avg, is_active
            ) VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s)
        """
        delhi_point_wkt = "POINT(28.5300 77.2150)"
        
        merchants_data = [
            ("RESTAURANT-KITCHEN-01", "Central Kitchen Delhi 01", "+111222333", delhi_point_wkt, "North Indian, Fast Food", "09:00-22:00", 4.5, 1)
        ]
        cursor.executemany(merchant_query, merchants_data)

        # 5. SEED INVENTORY CATALOG & STOCK LEVELS
        print("📦 Seeding Inventory Catalog...")
        catalog_query = """
            INSERT INTO inventory_catalog (product_id, provider_id, product_name, price, provider_type)
            VALUES (%s, %s, %s, %s, %s)
        """
        catalog_data = [
            ("PROD-001", "DARKSTORE-DELHI-01", "Fresh Organic Milk", 60.00, "MFC"),
            ("PROD-002", "DARKSTORE-DELHI-01", "Brown Bread", 45.00, "MFC")
        ]
        cursor.executemany(catalog_query, catalog_data)

        print("⚡ Seeding Stock Levels...")
        stock_query = """
            INSERT INTO stock_levels (dark_store_id, product_id, quantity_on_hand)
            VALUES (%s, %s, %s)
        """
        stock_data = [
            ("DARKSTORE-DELHI-01", "PROD-001", 100),
            ("DARKSTORE-DELHI-01", "PROD-002", 100)
        ]
        cursor.executemany(stock_query, stock_data)

        # 6. SEED MERCHANT MENUS
        print("🍽️ Seeding Merchant Menus...")
        menu_query = """
            INSERT INTO merchant_menus (menu_item_id, merchant_id, item_name, price)
            VALUES (%s, %s, %s, %s)
        """
        menu_data = [
            ("MENU-001", "RESTAURANT-KITCHEN-01", "Special Chicken Biryani", 250.00),
            ("MENU-002", "RESTAURANT-KITCHEN-01", "Paneer Butter Masala", 200.00)
        ]
        cursor.executemany(menu_query, menu_data)

        # 7. COMMIT THE WHOLE TRANSACTION AT ONCE
        conn.commit()
        print("\n🎉 SUCCESS: Seeding operations complete! All core database blueprints are 100% synchronized and live.")

    except mysql.connector.Error as err:
        print(f"❌ Database Transaction Failed: {err}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    seed_database()
