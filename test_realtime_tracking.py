import asyncio
import websockets
import json

AGENT_ID = "RIDER-0001"
BASE_WS_URL = "ws://127.0.0.1:8000"

async def customer_listener():
    """Simulates the customer tracking page listening live to updates."""
    uri = f"{BASE_WS_URL}/ws/customer/track/{AGENT_ID}"
    print(f"👥 [Customer Task] Connecting to tracking stream for {AGENT_ID}...")
    try:
        async with websockets.connect(uri) as websocket:
            print("👥 [Customer Task] Subscribed! Waiting for live rider updates...\n")
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                print(f"📡 [Customer Screen API Alert] -> Live Position Received!")
                print(json.dumps(data, indent=2))
                print("-" * 50)
    except Exception as e:
        print(f"❌ Customer listener error: {e}")

async def rider_streamer():
    """Simulates the delivery rider's mobile phone streaming GPS metrics."""
    uri = f"{BASE_WS_URL}/ws/rider/{AGENT_ID}"
    print(f"🏍️ [Rider Task] Connecting to stream tunnel...")
    await asyncio.sleep(2)  # Give the customer listener a head start to open its channel
    
    # Mock route path points representing physical movement
    mock_route_coordinates = [
        {"latitude": 28.6145, "longitude": 77.2095},
        {"latitude": 28.6150, "longitude": 77.2100},
        {"latitude": 28.6155, "longitude": 77.2105},
        {"latitude": 28.6160, "longitude": 77.2110}
    ]
    
    try:
        async with websockets.connect(uri) as websocket:
            for idx, pt in enumerate(mock_route_coordinates, start=1):
                print(f"🏍️ [Rider Task] Sending GPS Ping #{idx}: {pt}")
                await websocket.send(json.dumps(pt))
                
                # Wait for server ACK
                ack = await websocket.recv()
                
                # Wait 3 seconds before sending the next movement update step
                await asyncio.sleep(3)
    except Exception as e:
        print(f"❌ Rider streamer error: {e}")

async def main():
    # Run both the rider streaming and customer tracking tasks concurrently
    await asyncio.gather(
        customer_listener(),
        rider_streamer()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nSimulation stopped.")
