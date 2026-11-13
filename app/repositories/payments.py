"""Database operations; every method finishes its transaction before returning."""
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db import Session
from app.models import Outbox, Payment
from app.schemas import CreatePayment, PaymentCreated, PaymentDetails


class IdempotencyConflict(Exception):
    pass


def details(payment: Payment) -> PaymentDetails:
    return PaymentDetails(
        payment_id=payment.id, amount=payment.amount, currency=payment.currency,
        description=payment.description, metadata=payment.metadata_, webhook_url=payment.webhook_url,
        status=payment.status, created_at=payment.created_at, processed_at=payment.processed_at,
        delivery_state=payment.delivery_state,
    )


async def create(body: CreatePayment, key: str, digest: str) -> PaymentCreated:
    async with Session.begin() as session:
        stmt = (insert(Payment).values(
            amount=body.amount, currency=body.currency, description=body.description,
            metadata_=body.metadata, webhook_url=str(body.webhook_url),
            idempotency_key=key, request_hash=digest,
        ).on_conflict_do_nothing(index_elements=[Payment.idempotency_key]).returning(Payment))
        payment = (await session.execute(stmt)).scalar_one_or_none()
        if payment is None:
            payment = (await session.execute(
                select(Payment).where(Payment.idempotency_key == key)
            )).scalar_one()
            if payment.request_hash != digest:
                raise IdempotencyConflict
        else:
            session.add(Outbox(topic="payments.new", payload={
                "payment_id": str(payment.id), "attempt": 1,
            }))
        return PaymentCreated(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at,
        )


async def get(payment_id: UUID) -> PaymentDetails | None:
    async with Session() as session:
        payment = await session.get(Payment, payment_id)
        return details(payment) if payment else None


async def pending_attempt(payment_id: UUID, attempt: int) -> PaymentDetails | None:
    async with Session() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise ValueError("Unknown payment")
        if payment.delivery_state != "pending" or attempt <= payment.attempts:
            return None
        if attempt != payment.attempts + 1:
            raise ValueError("Out-of-order retry")
        return details(payment)


async def store_gateway_result(payment_id: UUID, status: str) -> PaymentDetails:
    async with Session.begin() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment.status == "pending":
            payment.status = status
            payment.processed_at = datetime.now(UTC)
        return details(payment)


async def complete_attempt(payment_id: UUID, attempt: int, *, error: str | None,
                           retry_base: float, payload: dict):
    """CAS under a short row lock: one retry/dead event per logical attempt."""
    async with Session.begin() as session:
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment.delivery_state != "pending" or attempt <= payment.attempts:
            return
        if attempt != payment.attempts + 1:
            raise ValueError("Out-of-order retry")
        payment.attempts = attempt
        if error is None:
            payment.delivery_state = "delivered"
            payment.webhook_sent_at = datetime.now(UTC)
        elif attempt >= 3:
            if payment.status == "pending":
                payment.status = "failed"
                payment.processed_at = datetime.now(UTC)
            payment.delivery_state = "dead"
            session.add(Outbox(topic="payments.dead", payload={
                **payload, "payment_id": str(payment.id), "status": payment.status,
                "attempts": attempt, "reason": error,
            }))
        else:
            session.add(Outbox(
                topic="payments.new",
                payload={"payment_id": str(payment.id), "attempt": attempt + 1},
                available_at=datetime.now(UTC) + timedelta(seconds=retry_base * 2 ** (attempt - 1)),
            ))
