from fastapi import FastAPI
from typing import List

app = FastAPI()

EVENTS: List[dict] = []

@app.post("/publish")
def publish_event(event: dict):
    EVENTS.append(event)
    return {"status": "accepted", "stored_events": len(EVENTS)}

@app.get("/events")
def get_events():
    return EVENTS