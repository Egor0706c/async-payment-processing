"""Local demonstration receiver; not part of the payment consumer."""

from fastapi import FastAPI, Request, Response

app = FastAPI()
events: dict[str, list] = {}


@app.post("/webhook/{mode}")
async def receive(mode: str, request: Request):
    event = await request.json()
    received = events.setdefault(event["payment_id"], [])
    received.append(event)
    if mode == "fail" or (mode == "flaky" and len(received) < 3):
        return Response(status_code=503)
    return Response(status_code=204)


@app.get("/events/{payment_id}")
async def received(payment_id: str):
    return events.get(payment_id, [])
