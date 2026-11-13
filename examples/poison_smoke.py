"""Verify malformed broker messages reach DLQ rather than requeue forever."""
import asyncio
import json
from uuid import uuid4

import httpx


async def main():
    marker = str(uuid4())
    base = "http://127.0.0.1:15673/api"
    async with httpx.AsyncClient(auth=("payments", "payments"), timeout=10) as client:
        response = await client.post(f"{base}/exchanges/%2F/payments/publish", json={
            "routing_key": "payments.new", "payload_encoding": "string",
            "properties": {"delivery_mode": 2, "message_id": marker},
            "payload": json.dumps({"payment_id": "invalid-uuid", "attempt": 1}),
        })
        response.raise_for_status()
        assert response.json()["routed"], response.text
        for _ in range(40):
            response = await client.post(f"{base}/queues/%2F/payments.dlq/get", json={
                "count": 100, "ackmode": "ack_requeue_true", "encoding": "auto",
            })
            response.raise_for_status()
            hit = next((m for m in response.json()
                        if m.get("properties", {}).get("message_id") == marker), None)
            if hit:
                deaths = hit.get("properties", {}).get("headers", {}).get("x-death", [])
                assert any(d.get("reason") == "delivery_limit" for d in deaths), hit
                print(json.dumps({"poison_message": marker, "outcome": "DLQ", "deaths": deaths}))
                return
            await asyncio.sleep(.5)
    raise AssertionError("Malformed message did not reach DLQ within 20 seconds")


asyncio.run(main())
