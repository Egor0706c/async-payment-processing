import asyncio
import random
from uuid import UUID

from app.config import settings


async def charge(payment_id: UUID) -> str:
    await asyncio.sleep(random.uniform(settings.gateway_min_seconds, settings.gateway_max_seconds))
    # Stable provider idempotency: redelivery cannot change a payment's outcome.
    return "succeeded" if payment_id.int % 10 < 9 else "failed"
