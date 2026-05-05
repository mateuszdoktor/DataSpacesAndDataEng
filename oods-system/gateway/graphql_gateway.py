import json
import requests
import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
import os
import uvicorn

with open(os.path.join(os.path.dirname(__file__), "../contracts/providers_registry.json"), "r") as f:
    PROVIDERS = json.load(f)

@strawberry.type
class Observation:
    provider: str
    timestamp: str
    object_id: str
    temperature: float
    velocity: float

def fetch_all():
    results = []
    for provider in PROVIDERS:
        provider_name = provider["name"]
        provider_url = provider["url"]
        try:
            response = requests.get(f"{provider_url}/observations", timeout=2)
            data = response.json()
            for row in data:
                results.append(Observation(
                    provider=provider_name,
                    timestamp=str(row["timestamp"]),
                    object_id=str(row["object_id"]),
                    temperature=float(row["temperature"]),
                    velocity=float(row["velocity"])
                ))
        except requests.exceptions.RequestException:
            pass
    return results

def fetch_object(object_id: str):
    results = []
    for provider in PROVIDERS:
        provider_name = provider["name"]
        provider_url = provider["url"]
        try:
            response = requests.get(f"{provider_url}/observations/{object_id}", timeout=2)
            data = response.json()
            for row in data:
                results.append(Observation(
                    provider=provider_name,
                    timestamp=str(row["timestamp"]),
                    object_id=str(row["object_id"]),
                    temperature=float(row["temperature"]),
                    velocity=float(row["velocity"])
                ))
        except requests.exceptions.RequestException:
            pass
    return results

@strawberry.type
class Query:
    @strawberry.field
    def observations(self) -> list[Observation]:
        return fetch_all()
    @strawberry.field
    def observations_by_object(self, object_id: str) -> list[Observation]:
        return fetch_object(object_id)

schema = strawberry.Schema(query=Query)
app = FastAPI()
app.include_router(GraphQLRouter(schema), prefix="/graphql")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9000)
