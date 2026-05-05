from fastapi import FastAPI
from fastapi.responses import FileResponse
import pandas as pd

app = FastAPI()

df = pd.read_csv("observations.csv")

@app.get("/")
def root():
    return {"status": "provider_online", "provider": "staellite_B"}

@app.get("/observations")
def get_observations():
    return df.to_dict(orient="records")

@app.get("/observations/{object_id}")
def get_object_observations(object_id: str):
    filtered = df[df["object_id"] == object_id]
    return filtered.to_dict(orient="records")

@app.get("/observations.csv")
def get_csv():
    return FileResponse("observations.csv", media_type="text/csv")