from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

broker = RabbitBroker(settings.rabbitmq_url)
exchange = RabbitExchange("payments", durable=True)
dlq = RabbitQueue("payments.dlq", durable=True, routing_key="payments.dead")
queue = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-queue-type": "quorum",
        "x-delivery-limit": 3,
        "x-dead-letter-exchange": "payments",
        "x-dead-letter-routing-key": "payments.dead",
    },
)


async def topology():
    declared_exchange = await broker.declare_exchange(exchange)
    for q in (queue, dlq):
        declared_queue = await broker.declare_queue(q)
        await declared_queue.bind(declared_exchange, routing_key=q.routing_key)
