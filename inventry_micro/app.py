from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json

app = FastAPI()

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    group_id="inventory_group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

def consume_messages():
    for message in consumer:
        order = message.value
        print(f"Processing order: {order}")
        # Simulate inventory update logic here

# Run the consumer in a background thread
threading.Thread(target=consume_messages, daemon=True).start()

@app.get("/")
async def root():
    return {"message": "Inventory Service is running"}
