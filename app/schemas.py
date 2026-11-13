from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl, field_validator


class CreatePayment(BaseModel):
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(default="", max_length=2000)
    metadata: dict = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("webhook_url")
    @classmethod
    def plain_http_url(cls, value):
        if value.username or value.password or value.fragment:
            raise ValueError("Credentials and fragments are not allowed in webhook URL")
        return value


class PaymentCreated(BaseModel):
    payment_id: UUID
    status: str
    created_at: datetime


class PaymentDetails(PaymentCreated):
    amount: Decimal
    currency: str
    description: str
    metadata: dict
    webhook_url: str
    processed_at: datetime | None
    delivery_state: str
