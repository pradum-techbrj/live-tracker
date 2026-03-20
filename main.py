import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as redis

app = FastAPI()

REDIS_URL = "redis://default:apFYzhCg7Q8crc85dWxkAep46IduIg9L@redis-11366.crce179.ap-south-1-1.ec2.cloud.redislabs.com:11366"

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

connections = {}


@app.websocket("/ws/{client_type}/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_type: str, client_id: str):
    await websocket.accept()

    try:
        pubsub = redis_client.pubsub()
        await pubsub.subscribe("location_updates")

        async def listen_redis():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    rider_id = data["riderId"]

                    if rider_id in connections:
                        for conn in connections[rider_id]:
                            try:
                                await conn.send_text(json.dumps(data))
                            except:
                                pass

        listener_task = asyncio.create_task(listen_redis())

        try:
            while True:
                data = await websocket.receive_text()
                message = json.loads(data)

                # Rider sends location
                if client_type == "rider":
                    payload = {
                        "riderId": client_id,
                        "lat": message["lat"],
                        "lng": message["lng"]
                    }
                    
                    await redis_client.set(f"rider:{client_id}", json.dumps(payload))
                    await redis_client.publish("location_updates", json.dumps(payload))

                # Customer subscribes
                elif client_type == "customer":
                    rider_id = message.get("riderId")

                    if rider_id:
                        connections.setdefault(rider_id, []).append(websocket)

        except WebSocketDisconnect:
            listener_task.cancel()

            for rider_id in list(connections.keys()):
                if websocket in connections[rider_id]:
                    connections[rider_id].remove(websocket)
    except Exception as e:
        print("error: ", str(e))
    finally:
        print("connection closed")
        
@app.get("/health")
async def health():
    return {"status": "ok"}