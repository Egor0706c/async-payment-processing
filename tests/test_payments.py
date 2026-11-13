import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import delete, func, select

from app import outbox
from app.api import app
from app.db import Session
from app.models import Outbox, Payment
from app.services import payments as service
from app.worker import Event, process

HEADERS = {"X-API-Key": "local-demo-change-me"}
BODY = {
    "amount": "120.50",
    "currency": "RUB",
    "description": "Test",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://merchant.example/webhook",
}


@pytest_asyncio.fixture(autouse=True)
async def clean():
    async with Session.begin() as s:
        await s.execute(delete(Outbox))
        await s.execute(delete(Payment))
    yield


@pytest_asyncio.fixture
async def client():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as c:
        yield c


async def create(client, key=None, **changes):
    return await client.post(
        "/api/v1/payments",
        headers={**HEADERS, "Idempotency-Key": key or str(uuid4())},
        json={**BODY, **changes},
    )


async def test_auth_validation_and_not_found(client):
    assert (await client.get("/health")).status_code == 401
    assert (await client.get("/health", headers=HEADERS)).status_code == 200
    assert (await client.post("/api/v1/payments", json=BODY)).status_code == 401
    assert (await client.post("/api/v1/payments", headers=HEADERS, json=BODY)).status_code == 422
    for change in (
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"currency": "BTC"},
        {"webhook_url": "file:///etc/passwd"},
        {"webhook_url": "https://a:b@example.com/x"},
    ):
        assert (await create(client, **change)).status_code == 422
    assert (await client.get(f"/api/v1/payments/{uuid4()}", headers=HEADERS)).status_code == 404


async def test_atomic_create_idempotency_and_conflict(client):
    a = await create(client, "same", amount="120.5")
    b = await create(client, "same")
    assert a.status_code == b.status_code == 202
    assert a.json()["payment_id"] == b.json()["payment_id"]
    assert (await create(client, "same", amount="121")).status_code == 409
    async with Session() as s:
        assert await s.scalar(select(func.count()).select_from(Payment)) == 1
        assert await s.scalar(select(func.count()).select_from(Outbox)) == 1
    details = await client.get("/api/v1/payments/" + a.json()["payment_id"], headers=HEADERS)
    assert details.json()["metadata"] == BODY["metadata"]
    assert details.json()["status"] == "pending"


async def test_concurrent_duplicate_requests(client):
    responses = await asyncio.gather(*(create(client, "concurrent") for _ in range(12)))
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with Session() as s:
        assert await s.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.parametrize("gateway_status", ["succeeded", "failed"])
async def test_gateway_outcome_and_duplicate_consume(client, monkeypatch, gateway_status):
    pid = (await create(client)).json()["payment_id"]
    calls = []

    async def gateway(payment_id):
        calls.append("gateway")
        return gateway_status

    async def post(self, url, **kwargs):
        calls.append(kwargs["json"])
        return httpx.Response(200)

    monkeypatch.setattr(service.gateway, "charge", gateway)
    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    await process(Event(payment_id=pid, attempt=1))
    await process(Event(payment_id=pid, attempt=1))
    async with Session() as s:
        p = await s.get(Payment, pid)
        assert p.status == gateway_status and p.delivery_state == "delivered"
        assert p.processed_at and p.webhook_sent_at
    assert len(calls) == 2
    assert calls[1]["status"] == gateway_status


async def test_three_attempts_backoff_dlq_and_no_reprocessing(client, monkeypatch):
    pid = (await create(client)).json()["payment_id"]
    gateway_calls = []

    async def gateway(payment_id):
        gateway_calls.append(payment_id)
        return "succeeded"

    async def post(self, url, **kwargs):
        raise httpx.ConnectError("temporary outage")

    monkeypatch.setattr(service.gateway, "charge", gateway)
    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    for attempt in (1, 2, 3, 3):
        before = datetime.now(UTC)
        await process(Event(payment_id=pid, attempt=attempt))
        if attempt < 3:
            async with Session() as s:
                events = (
                    await s.scalars(select(Outbox).where(Outbox.topic == "payments.new"))
                ).all()
                retry = next(e for e in events if e.payload["attempt"] == attempt + 1)
                assert (retry.available_at - before).total_seconds() >= 2 ** (attempt - 1)
    async with Session() as s:
        p = await s.get(Payment, pid)
        assert p.status == "succeeded" and p.delivery_state == "dead"
        assert p.attempts == 3
        assert (
            await s.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.topic == "payments.dead")
            )
            == 1
        )
    assert len(gateway_calls) == 1


async def test_retry_success_and_stale_event(client, monkeypatch):
    pid = (await create(client)).json()["payment_id"]

    async def gateway(payment_id):
        return "succeeded"

    responses = [503, 204]

    async def post(self, url, **kwargs):
        req = httpx.Request("POST", url)
        return httpx.Response(responses.pop(0), request=req)

    monkeypatch.setattr(service.gateway, "charge", gateway)
    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    await process(Event(payment_id=pid, attempt=1))
    await process(Event(payment_id=pid, attempt=1))  # redelivery does not spend a new attempt
    await process(Event(payment_id=pid, attempt=2))
    async with Session() as s:
        p = await s.get(Payment, pid)
        assert p.delivery_state == "delivered" and p.attempts == 2
    assert responses == []


async def test_outbox_publish_failure_rolls_back_marker(client, monkeypatch):
    await create(client)

    async def fail(*args, **kwargs):
        raise ConnectionError("broker unavailable")

    monkeypatch.setattr(outbox.broker, "publish", fail)
    with pytest.raises(ConnectionError):
        await outbox.publish_batch()
    async with Session() as s:
        assert (
            await s.scalar(
                select(func.count()).select_from(Outbox).where(Outbox.published_at.is_(None))
            )
            == 1
        )

    async def success(*args, **kwargs):
        assert kwargs["persist"] and kwargs["mandatory"]

    monkeypatch.setattr(outbox.broker, "publish", success)
    assert await outbox.publish_batch() == 1
    assert await outbox.publish_batch() == 0


async def test_unexpected_gateway_errors_are_bounded_and_persisted(client, monkeypatch):
    pid = (await create(client)).json()["payment_id"]
    calls = []

    async def fail(payment_id):
        calls.append(payment_id)
        raise RuntimeError("unexpected gateway adapter failure")

    monkeypatch.setattr(service.gateway, "charge", fail)
    for attempt in (1, 1, 2, 3, 3):
        await process(Event(payment_id=pid, attempt=attempt))
    async with Session() as s:
        p = await s.get(Payment, pid)
        assert (p.status, p.delivery_state, p.attempts) == ("failed", "dead", 3)
        events = (await s.scalars(select(Outbox))).all()
        dead = [e for e in events if e.topic == "payments.dead"]
        assert len(dead) == 1
        assert dead[0].payload["reason"] == "processing_failed"
        assert sorted(e.payload["attempt"] for e in events if e.topic == "payments.new") == [1, 2, 3]
    assert len(calls) == 3


async def test_external_calls_do_not_hold_payment_row_locks(client, monkeypatch):
    pid = (await create(client)).json()["payment_id"]
    stages = []

    async def check_unlocked(stage):
        async with Session.begin() as s:
            # NOWAIT fails immediately if another transaction keeps a row lock.
            from sqlalchemy import text
            await s.execute(text("SET LOCAL lock_timeout = '100ms'"))
            p = await s.scalar(select(Payment).where(Payment.id == pid).with_for_update(nowait=True))
            assert p is not None
        stages.append(stage)

    async def gateway(payment_id):
        await check_unlocked("gateway")
        return "succeeded"

    async def post(self, url, **kwargs):
        await check_unlocked("webhook")
        return httpx.Response(204)

    monkeypatch.setattr(service.gateway, "charge", gateway)
    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    await process(Event(payment_id=pid, attempt=1))
    assert stages == ["gateway", "webhook"]
    async with Session() as s:
        p = await s.get(Payment, pid)
        assert p.delivery_state == "delivered"


async def test_unexpected_webhook_adapter_error_does_not_recharge(client, monkeypatch):
    pid = (await create(client)).json()["payment_id"]
    calls = []

    async def gateway(payment_id):
        calls.append(payment_id)
        return "succeeded"

    async def post(self, url, **kwargs):
        raise RuntimeError("adapter bug, not an HTTPError")

    monkeypatch.setattr(service.gateway, "charge", gateway)
    monkeypatch.setattr(httpx.AsyncClient, "post", post)
    for attempt in (1, 2, 3):
        await process(Event(payment_id=pid, attempt=attempt))
    async with Session() as s:
        p = await s.get(Payment, pid)
        assert p.status == "succeeded" and p.delivery_state == "dead" and p.attempts == 3
    assert len(calls) == 1
