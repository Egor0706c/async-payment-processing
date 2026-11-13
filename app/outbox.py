import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import func, select

from app.db import Session
from app.messaging import broker, exchange
from app.models import Outbox

log = logging.getLogger(__name__)


async def publish_batch():
    async with Session.begin() as session:
        rows = (
            await session.scalars(
                select(Outbox)
                .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
                .order_by(Outbox.available_at)
                .limit(50)
                .with_for_update(skip_locked=True)
            )
        ).all()
        for event in rows:
            # Publisher confirms + mandatory routing; only mark after broker acknowledgment.
            await broker.publish(
                event.payload,
                exchange=exchange,
                routing_key=event.topic,
                persist=True,
                mandatory=True,
                timeout=10,
                message_id=str(event.id),
            )
            event.published_at = datetime.now(UTC)
        return len(rows)


async def relay(stop: asyncio.Event):
    while not stop.is_set():
        try:
            await publish_batch()
        except Exception:
            log.exception("Outbox publication failed; rows remain pending")
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.5)
        except TimeoutError:
            pass
