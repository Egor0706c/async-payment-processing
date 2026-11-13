"""Payment use cases; no SQL, no transaction held over external I/O."""
import hashlib
import json
import logging
from uuid import UUID

from pydantic import BaseModel, Field

from app.config import settings
from app.integrations import gateway, webhooks
from app.repositories import payments as repo
from app.schemas import CreatePayment

log = logging.getLogger(__name__)


class Event(BaseModel):
    payment_id: UUID
    attempt: int = Field(ge=1, le=3)


async def create(body: CreatePayment, key: str):
    canonical = body.model_dump(mode="json")
    canonical["amount"] = format(body.amount, ".2f")
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return await repo.create(body, key, digest)


async def process(event: Event):
    payment = await repo.pending_attempt(event.payment_id, event.attempt)
    if payment is None:
        return
    payload = {"payment_id": str(event.payment_id)}
    stage = "processing_failed"
    try:
        if payment.status == "pending":
            status = await gateway.charge(event.payment_id)
            payment = await repo.store_gateway_result(event.payment_id, status)
        payload = {
            "event_id": f"payment:{payment.payment_id}:result",
            "payment_id": str(payment.payment_id), "status": payment.status,
            "amount": format(payment.amount, ".2f"), "currency": payment.currency,
            "processed_at": payment.processed_at.isoformat(),
        }
        stage = "webhook_delivery_failed"
        await webhooks.deliver(payment.webhook_url, payload)
    except Exception:
        # Includes gateway, serialization and unexpected adapter errors, not only HTTP errors.
        # If DB itself is unavailable, recording fails and the message is NACKed by transport.
        # Cancellation is BaseException: leave message unacknowledged for broker redelivery.
        log.exception("Payment %s attempt %s failed", event.payment_id, event.attempt)
        await repo.complete_attempt(
            event.payment_id, event.attempt, error=stage,
            retry_base=settings.retry_base_seconds, payload=payload,
        )
    else:
        await repo.complete_attempt(
            event.payment_id, event.attempt, error=None,
            retry_base=settings.retry_base_seconds, payload=payload,
        )
