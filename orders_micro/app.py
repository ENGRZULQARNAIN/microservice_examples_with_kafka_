from fastapi import FastAPI
from kafka import KafkaProducer
import json

app = FastAPI(title="MICRO SERVICE ORDER")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.post("/order/")
async def create_order(order_id: int, product: str, quantity: int):
    order = {"order_id": order_id, "product": product, "quantity": quantity}
    producer.send("orders", order)
    return {"message": "Order sent to Kafka", "order": order}
