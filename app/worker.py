"""RabbitMQ transport adapter: one handler delegates to a payment use case."""
import asyncio

from faststream import AckPolicy, FastStream

from app.db import engine
from app.messaging import broker, exchange, queue, topology
from app.outbox import relay
from app.services.payments import Event, process

app = FastStream(broker)
stop = asyncio.Event()
relay_task = None


@broker.subscriber(queue, exchange=exchange, ack_policy=AckPolicy.NACK_ON_ERROR)
async def consume(body: dict):
    await process(Event.model_validate(body))


@app.after_startup
async def startup():
    global relay_task
    await topology()
    relay_task = asyncio.create_task(relay(stop))


@app.on_shutdown
async def shutdown():
    stop.set()
    if relay_task:
        await relay_task
    await engine.dispose()
