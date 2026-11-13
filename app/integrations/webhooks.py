import httpx


async def deliver(url: str, payload: dict):
    async with httpx.AsyncClient(timeout=10, follow_redirects=False, trust_env=False) as client:
        response = await client.post(
            url, json=payload, headers={"Idempotency-Key": payload["event_id"]},
        )
        if not 200 <= response.status_code < 300:
            raise httpx.HTTPStatusError(
                "Webhook rejected", request=response.request, response=response,
            )
