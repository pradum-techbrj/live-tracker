import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as redis
from collections import defaultdict

app = FastAPI()

REDIS_URL = "redis://default:ALlKGIYQsFNL1394vkLQpDWG0f2RNxcF@redis-17683.crce206.ap-south-1-1.ec2.cloud.redislabs.com:17683"

redis_client = redis.from_url(REDIS_URL, decode_responses=True)

connections = defaultdict(set)
pubsub = redis_client.pubsub()

@app.on_event("startup")
async def start_redis_listener():
    await pubsub.subscribe("location_updates")

    async def redis_listener():
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                data = json.loads(message["data"])
                rider_id = data.get("riderId")

                if not rider_id:
                    continue

                for ws in list(connections[rider_id]):
                    try:
                        await ws.send_text(json.dumps(data))
                    except:
                        connections[rider_id].discard(ws)

        except Exception as e:
            print("Redis listener error:", e)

    asyncio.create_task(redis_listener())

@app.websocket("/ws/{client_type}/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_type: str, client_id: str):
    await websocket.accept()

    try:
        while True:
            raw = await websocket.receive_text()
            message = json.loads(raw)

            if client_type == "rider":
                payload = {
                    "riderId": client_id,
                    "lat": message.get("lat"),
                    "lng": message.get("lng")
                }

                await redis_client.set(
                    f"rider:{client_id}",
                    json.dumps(payload),
                    ex=60
                )

                await redis_client.publish("location_updates", json.dumps(payload))

            elif client_type == "customer":
                rider_id = message.get("riderId")

                if rider_id:
                    connections[rider_id].add(websocket)

    except WebSocketDisconnect:
        pass

    finally:
        for rider_id in list(connections.keys()):
            connections[rider_id].discard(websocket)
            if not connections[rider_id]:
                del connections[rider_id]

        print("connection closed")


@app.get("/health")
async def health():
    return {"status": "ok"}