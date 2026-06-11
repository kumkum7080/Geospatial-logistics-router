import os
import uuid
import random
import pandas as pd
import mysql.connector
from datetime import datetime
from config import DB_CONFIG

def parse_rate(rate_val):
    if pd.isna(rate_val):
        return 4.0
    val_str = str(rate_val).strip()
    if 'NEW' in val_str or '-' in val_str:
        return 4.0
    try:
        parts = val_str.split('/')
        return float(parts[0].strip())
    except:
        return 4.0

# Rich, curated mapping of cuisines to actual realistic dishes
CUISINE_DISHES = {
    "north indian": [
        "Butter Chicken", "Paneer Butter Masala", "Garlic Naan", "Dal Makhani",
        "Tandoori Chicken", "Chole Bhature", "Kadai Paneer", "Malai Kofta",
        "Palak Paneer", "Shahi Paneer", "Aloo Gobhi", "Lacha Naan", "Jeera Rice"
    ],
    "south indian": [
        "Masala Dosa", "Idli Sambar", "Medu Vada", "Rava Onion Dosa",
        "Filter Coffee", "Pongal", "Kesari Bath", "Lemon Rice",
        "Bisi Bele Bath", "Appam with Veg Stew", "Ghee Roast Dosa", "Mysore Masala Dosa"
    ],
    "chinese": [
        "Veg Momos", "Chicken Fried Rice", "Hakka Noodles", "Gobi Manchurian",
        "Chilli Chicken", "Spring Rolls", "Manchow Soup", "Schezwan Fried Rice",
        "Drums of Heaven", "Sweet and Sour Chicken", "Veg Chowmein"
    ],
    "thai": [
        "Thai Green Curry", "Thai Red Curry", "Pad Thai Noodles", "Tom Yum Soup",
        "Pineapple Fried Rice"
    ],
    "italian": [
        "Margherita Pizza", "Alfredo Pasta", "Arrabbiata Pasta", "Garlic Bread with Cheese",
        "Bruschetta", "Lasagna", "Pesto Pasta", "Tiramisu"
    ],
    "pizza": [
        "Margherita Pizza", "Farmhouse Pizza", "Peppy Paneer Pizza", "Chicken Tikka Pizza",
        "Double Cheese Margherita", "Veg Supreme Pizza", "Garlic Breadsticks"
    ],
    "cafe": [
        "Cappuccino", "Cafe Latte", "Cold Coffee", "Churros", "Garlic Bread",
        "Cheese Sandwich", "French Fries", "Hot Chocolate", "Croissant", "Club Sandwich"
    ],
    "mexican": [
        "Tacos", "Quesadilla", "Burrito", "Nachos with Cheese Sauce", "Churros"
    ],
    "biryani": [
        "Chicken Dum Biryani", "Mutton Biryani", "Veg Dum Biryani", "Egg Biryani",
        "Donne Chicken Biryani", "Ambur Biryani", "Kolkata Style Biryani"
    ],
    "andhra": [
        "Andhra Chicken Curry", "Guntur Chicken Fry", "Andhra Style Pappu", "Nellore Fish Curry",
        "Chicken Ghee Roast"
    ],
    "bengali": [
        "Luchi with Alur Dom", "Kosha Mangsho", "Sondesh", "Roshogolla", "Fish Cutlet"
    ],
    "desserts": [
        "Chocolate Lava Cake", "Sizzling Brownie", "Gulab Jamun", "Rasmalai",
        "Mango Kulfi", "Oreo Milkshake", "Butterscotch Shake"
    ],
    "beverages": [
        "Fresh Lime Soda", "Mango Lassi", "Sweet Lassi", "Mint Mojito",
        "Iced Tea", "Masala Chai", "Cold Coffee"
    ]
}

FALLBACK_POOL = [
    "Filter Coffee & Idli Combo", "Masala Dosa with Chutney", "Bangalore Style Biryani",
    "Kesari Bath", "Paneer Butter Masala", "Butter Chicken", "Garlic Naan", "Veg Fried Rice",
    "Gobi Manchurian", "Churros", "Chocolate Lava Cake", "Hot Chocolate"
]

def seed_perfect_zomato_data():
    csv_path = r"C:\Users\kumku\.gemini\antigravity\scratch\geospatial_routing_system\archive (2)\zomato.csv"
    print(f"📖 Reading rich Bangalore Zomato dataset from {csv_path}...")
    
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found at {csv_path}")
        return
        
    try:
        df = pd.read_csv(csv_path)
        df = df.dropna(subset=['name'])
        print(f"Successfully loaded CSV. Total rows: {len(df)}")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    print("🔌 Connecting to MySQL database system...")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Clear old data to prevent foreign key errors
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    cursor.execute("TRUNCATE TABLE merchant_menus;")
    cursor.execute("TRUNCATE TABLE merchant_partners_restaurants;")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
    print("🧹 Database tables (merchant_partners_restaurants, merchant_menus) scrubbed clean.")

    # Core coordinates for Bangalore center (M.G. Road / Brigade Road hub)
    base_lat = 12.9716
    base_lng = 77.5946

    # We will sample 500 restaurants for a dense, rich Bangalore cluster
    sample_size = min(500, len(df))
    df_sample = df.sample(n=sample_size, random_state=42)

    success_merchants = 0
    success_items = 0

    print(f"🚀 Ingesting {sample_size} restaurants and generating Bangalore geospatial points...")

    insert_merchant_query = """
        INSERT INTO merchant_partners_restaurants (
            merchant_id, name, contact_details, spatial_point, cuisine_types, open_closed_hours, rating_avg, is_active
        ) VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, 1)
    """

    insert_menu_query = """
        INSERT INTO merchant_menus (menu_item_id, merchant_id, item_name, price)
        VALUES (%s, %s, %s, %s)
    """

    for idx, row in df_sample.iterrows():
        merchant_id = f"RESTAURANT-KITCHEN-{uuid.uuid4().hex[:6].upper()}"
        rest_name = str(row['name'])[:150]
        
        # Phone
        contact = str(row['phone'])[:45] if 'phone' in df.columns and pd.notna(row['phone']) else "+91 99999 99999"
        
        # Cuisines
        cuisines = "North Indian, South Indian"
        if 'cuisines' in df.columns and pd.notna(row['cuisines']):
            cuisines = str(row['cuisines'])[:240]

        # Rate
        rate = parse_rate(row.get('rate'))

        # Scatter restaurants within a 5-6km radius of downtown Bangalore (lat, lng order)
        lat_offset = random.uniform(-0.045, 0.045)
        lng_offset = random.uniform(-0.045, 0.045)
        lat = base_lat + lat_offset
        lng = base_lng + lng_offset
        wkt_point = f"POINT({lat} {lng})"

        # Hours
        hours = "09:00-23:00"

        try:
            # 1. Insert Merchant
            cursor.execute(insert_merchant_query, (
                merchant_id, rest_name, contact, wkt_point, cuisines, hours, rate
            ))
            success_merchants += 1

            # 2. Extract Dishes
            dishes = []
            
            # Start with real liked dishes if available
            if 'dish_liked' in df.columns and pd.notna(row['dish_liked']):
                liked = [d.strip() for d in str(row['dish_liked']).split(',')]
                for d in liked:
                    if d and d.lower() != 'nan' and d not in dishes:
                        dishes.append(d)
            
            # Extract and parse raw menu items list if populated
            if 'menu_item' in df.columns and pd.notna(row['menu_item']) and str(row['menu_item']) != "[]":
                clean_menu = str(row['menu_item']).replace("[", "").replace("]", "").replace("'", "")
                menu_parts = [d.strip() for d in clean_menu.split(',')]
                for d in menu_parts:
                    if d and d.lower() != 'nan' and d not in dishes:
                        dishes.append(d)

            # Build cuisine-specific dynamic dishes
            cuisine_pool = []
            cuisines_lower = cuisines.lower()
            for key, items in CUISINE_DISHES.items():
                if key in cuisines_lower:
                    cuisine_pool.extend(items)
            
            # Shuffle the cuisine pool so different restaurants with the same cuisine get different items
            random.shuffle(cuisine_pool)
            
            # Append from cuisine pool
            for d in cuisine_pool:
                if d not in dishes:
                    dishes.append(d)

            # Deduplicate case-insensitively
            seen_lower = set()
            unique_dishes = []
            for d in dishes:
                d_lower = d.lower()
                if d_lower not in seen_lower:
                    seen_lower.add(d_lower)
                    unique_dishes.append(d)

            # If still have less than 6 dishes, pad from fallback pool
            shuffled_fallback = list(FALLBACK_POOL)
            random.shuffle(shuffled_fallback)
            for d in shuffled_fallback:
                if len(unique_dishes) >= 8:
                    break
                if d.lower() not in seen_lower:
                    seen_lower.add(d.lower())
                    unique_dishes.append(d)

            # Select 6 to 8 items per restaurant
            final_menu = unique_dishes[:8]

            # Insert dishes
            for dish in final_menu:
                if not dish:
                    continue
                menu_item_id = f"MENU-{uuid.uuid4().hex[:8].upper()}"
                dish_name = dish[:240]
                price = round(random.uniform(90.00, 380.00), 2)

                cursor.execute(insert_menu_query, (
                    menu_item_id, merchant_id, dish_name, price
                ))
                success_items += 1

        except Exception as e:
            print(f"⚠️ Warning: Row insertion skipped due to database error: {e}")
            continue

    conn.commit()
    print(f"\n🎉 Bangalore relational Zomato seed complete!")
    print(f"✅ Restaurants created: {success_merchants}")
    print(f"✅ Menu items seeded: {success_items}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    seed_perfect_zomato_data()