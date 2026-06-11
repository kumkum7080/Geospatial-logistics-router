import asyncio
import websockets
import json
import time

# Target the local FastAPI WebSocket URL for Rider-0001
ws_url = "ws://127.0.0.1:8000/ws/rider/RIDER-0001"

# Simulated delivery path coordinates representing a rider moving down the street in Delhi
simulated_gps_route = [
    {"latitude": 28.5200, "longitude": 77.2150},
    {"latitude": 28.5215, "longitude": 77.2165},
    {"latitude": 28.5230, "longitude": 77.2180},
    {"latitude": 28.5242, "longitude": 77.2192},
    {"latitude": 28.5250, "longitude": 77.2200}  # Arrived at destination waypoint
]

async def stream_rider_telemetry():
    print(f"📡 Establishing persistent socket connection to: {ws_url}")
    try:
        async with websockets.connect(ws_url) as websocket:
            print("✅ Tunnel established successfully! Beginning live coordinate streaming...\n")
            print("="*85)
            
            for step, waypoint in enumerate(simulated_gps_route, 1):
                print(f"✈️ Step {step}/{len(simulated_gps_route)}: Pushing live location -> Lat: {waypoint['latitude']}, Lng: {waypoint['longitude']}")
                
                # Send telemetry up the WebSocket pipe
                await websocket.send(json.dumps(waypoint))
                
                # Wait for the instant server response (ACK)
                response = await websocket.recv()
                server_ack = json.loads(response)
                print(f"📥 Server Response Received: {server_ack}")
                print("-" * 50)
                
                # Pause for 2 seconds to simulate a real driving movement interval
                await asyncio.sleep(2)
                
            print("\n🏁 Ride route execution completed. Terminating track connection.")
            
    except Exception as e:
        print(f"❌ WebSocket Client Error: {e}")

if __name__ == "__main__":
    # Launch the asynchronous loop runtime
    asyncio.run(stream_rider_telemetry())