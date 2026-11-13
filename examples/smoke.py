"""End-to-end verification with actual API, worker, Postgres, RabbitMQ and HTTP callbacks."""

import asyncio
import json
import os
from uuid import uuid4

import httpx


async def main():
    headers = {"X-API-Key": os.getenv("API_KEY", "local-demo-change-me")}
    results = []
    async with httpx.AsyncClient(timeout=10, trust_env=False) as c:
        for mode, delivery, attempts in [
            ("ok", "delivered", 1),
            ("flaky", "delivered", 3),
            ("fail", "dead", 3),
        ]:
            key = str(uuid4())
            body = {
                "amount": "150.00",
                "currency": "RUB",
                "metadata": {"test": mode},
                "webhook_url": f"http://webhook:8001/webhook/{mode}",
            }
            a = await c.post(
                "http://127.0.0.1:8000/api/v1/payments",
                json=body,
                headers={**headers, "Idempotency-Key": key},
            )
            assert a.status_code == 202, a.text
            pid = a.json()["payment_id"]
            b = await c.post(
                "http://127.0.0.1:8000/api/v1/payments",
                json=body,
                headers={**headers, "Idempotency-Key": key},
            )
            assert b.status_code == 202 and b.json()["payment_id"] == pid
            for _ in range(90):
                response = await c.get(
                    f"http://127.0.0.1:8000/api/v1/payments/{pid}", headers=headers
                )
                response.raise_for_status()
                p = response.json()
                if p["delivery_state"] == delivery:
                    break
                await asyncio.sleep(0.5)
            assert p["delivery_state"] == delivery, p
            events = (await c.get(f"http://127.0.0.1:8001/events/{pid}")).json()
            assert len(events) == attempts, events
            assert len({event["event_id"] for event in events}) == 1
            results.append(
                {
                    "mode": mode,
                    "payment_id": pid,
                    "status": p["status"],
                    "delivery_state": delivery,
                    "callbacks": len(events),
                }
            )
        # Inspect DLQ without consuming (RabbitMQ Management API).
        for _ in range(20):
            r = await c.get(
                "http://127.0.0.1:15673/api/queues/%2F/payments.dlq", auth=("payments", "payments")
            )
            if r.status_code == 200 and r.json().get("messages", 0) >= 1:
                break
            await asyncio.sleep(1)
        assert r.status_code == 200 and r.json()["messages"] >= 1, r.text
        print(json.dumps({"scenarios": results, "dlq_messages": r.json()["messages"]}, indent=2))


asyncio.run(main())
