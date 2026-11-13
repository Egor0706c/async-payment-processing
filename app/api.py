import secrets
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import engine, session_dependency
from app.repositories import payments as repo
from app.schemas import CreatePayment, PaymentCreated, PaymentDetails
from app.services import payments as service


@asynccontextmanager
async def lifespan(app):
    yield
    await engine.dispose()


app = FastAPI(title="Async Payment Processing", lifespan=lifespan)


def authenticate(x_api_key: Annotated[str | None, Header()] = None):
    if not x_api_key or not secrets.compare_digest(x_api_key.encode(), settings.api_key.encode()):
        raise HTTPException(401, "Invalid API key")


@app.get("/health", include_in_schema=False, dependencies=[Depends(authenticate)])
async def health(session: Annotated[AsyncSession, Depends(session_dependency)]):
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.post("/api/v1/payments", status_code=202, response_model=PaymentCreated,
          dependencies=[Depends(authenticate)])
async def create_payment(
    body: CreatePayment,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
):
    try:
        return await service.create(body, idempotency_key)
    except repo.IdempotencyConflict:
        raise HTTPException(409, "Idempotency-Key already used with different data") from None


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails,
         dependencies=[Depends(authenticate)])
async def get_payment(payment_id: UUID):
    payment = await repo.get(payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return payment
