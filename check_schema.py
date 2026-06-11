import pymysql
from config import DB_CONFIG

conn = pymysql.connect(
    **DB_CONFIG
)
cursor = conn.cursor()

print("📋 Checking 'merchant_partners_restaurants' columns:")
cursor.execute("DESCRIBE merchant_partners_restaurants;")
for row in cursor.fetchall():
    print(f"Column: {row[0]} | Type: {row[1]}")

print("\n📋 Checking 'delivery_partners_drivers' columns:")
cursor.execute("DESCRIBE delivery_partners_drivers;")
for row in cursor.fetchall():
    print(f"Column: {row[0]} | Type: {row[1]}")

cursor.close()
conn.close()
