import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("events-service")

PORT = int(os.getenv("PORT", "8082"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "events-service")

MOVIE_TOPIC = "movie-events"
USER_TOPIC = "user-events"
PAYMENT_TOPIC = "payment-events"
TOPICS = [MOVIE_TOPIC, USER_TOPIC, PAYMENT_TOPIC]


class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[List[str]] = None
    description: Optional[str] = None


class UserEvent(BaseModel):
    user_id: int
    username: Optional[str] = None
    email: Optional[str] = None
    action: str
    timestamp: datetime


class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime
    method_type: Optional[str] = None


app = FastAPI(title="CinemaAbyss Events Service")
state = {"producer": None, "consumer_task": None}


async def start_producer() -> AIOKafkaProducer:
    while True:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )
        try:
            await producer.start()
            logger.info("Kafka producer connected to %s", KAFKA_BROKERS)
            return producer
        except KafkaError as exc:
            logger.warning("Kafka producer not ready (%s), retrying in 5s", exc)
            await producer.stop()
            await asyncio.sleep(5)


async def consume():
    while True:
        consumer = AIOKafkaConsumer(
            *TOPICS,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        try:
            await consumer.start()
            logger.info("Kafka consumer subscribed to %s", ", ".join(TOPICS))
            async for message in consumer:
                event = message.value
                logger.info(
                    "Processed %s event id=%s topic=%s partition=%d offset=%d payload=%s",
                    event.get("type"),
                    event.get("id"),
                    message.topic,
                    message.partition,
                    message.offset,
                    json.dumps(event.get("payload"), ensure_ascii=False),
                )
        except asyncio.CancelledError:
            await consumer.stop()
            raise
        except Exception as exc:
            logger.warning("Kafka consumer error (%s), restarting in 5s", exc)
            await consumer.stop()
            await asyncio.sleep(5)


async def publish(topic: str, event_type: str, event_id: str, payload: dict) -> JSONResponse:
    producer = state["producer"]
    if producer is None:
        return JSONResponse(status_code=500, content={"error": "Kafka producer is not ready"})
    event = {
        "id": event_id,
        "type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    try:
        metadata = await producer.send_and_wait(topic, value=event, key=event_id)
    except KafkaError as exc:
        logger.error("Failed to publish event %s: %s", event_id, exc)
        return JSONResponse(status_code=500, content={"error": str(exc)})
    logger.info(
        "Published %s event id=%s topic=%s partition=%d offset=%d",
        event_type,
        event_id,
        topic,
        metadata.partition,
        metadata.offset,
    )
    return JSONResponse(
        status_code=201,
        content={
            "status": "success",
            "partition": metadata.partition,
            "offset": metadata.offset,
            "event": event,
        },
    )


@app.on_event("startup")
async def on_startup():
    state["producer"] = await start_producer()
    state["consumer_task"] = asyncio.create_task(consume())


@app.on_event("shutdown")
async def on_shutdown():
    task = state["consumer_task"]
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    if state["producer"]:
        await state["producer"].stop()


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=400, content={"error": str(exc.errors())})


@app.get("/api/events/health")
async def health():
    return {"status": True}


@app.post("/api/events/movie", status_code=201)
async def create_movie_event(event: MovieEvent):
    event_id = f"movie-{event.movie_id}-{event.action}"
    return await publish(MOVIE_TOPIC, "movie", event_id, event.model_dump(mode="json", exclude_none=True))


@app.post("/api/events/user", status_code=201)
async def create_user_event(event: UserEvent):
    event_id = f"user-{event.user_id}-{event.action}"
    return await publish(USER_TOPIC, "user", event_id, event.model_dump(mode="json", exclude_none=True))


@app.post("/api/events/payment", status_code=201)
async def create_payment_event(event: PaymentEvent):
    event_id = f"payment-{event.payment_id}-{event.status}"
    return await publish(PAYMENT_TOPIC, "payment", event_id, event.model_dump(mode="json", exclude_none=True))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
