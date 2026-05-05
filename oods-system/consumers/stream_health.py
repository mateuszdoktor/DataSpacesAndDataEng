import requests
import json
import os

BROKER_EVENTS_URL = "http://127.0.0.1:7000/events"
base_path = ".." if os.path.basename(os.getcwd()) == "consumers" else "."
event_schema_path = os.path.join(base_path, "contracts/event_schema.json")
providers_registry_path = os.path.join(base_path, "contracts/providers_registry.json")

try:
    with open(providers_registry_path, "r") as f:
        expected_providers = [p["name"] for p in json.load(f)]
    with open(event_schema_path, "r") as f:
        required_fields = json.load(f).get("required_fields", [])
except:
    expected_providers = []
    required_fields = []

try:
    events = requests.get(BROKER_EVENTS_URL).json()
except:
    events = []

health = "GOOD"
reasons = []

if not events:
    print("STREAM HEALTH: CRITICAL\n- No events in the broker stream.")
    exit(0)

active_providers = set(e.get("provider") for e in events[-20:] if "provider" in e)
missing = set(expected_providers) - active_providers
if missing:
    health = "WARNING"
    reasons.append(f"Missing providers: {', '.join(missing)}")

invalid_count = sum(1 for e in events if any(f not in e for f in required_fields))
if invalid_count > 0:
    health = "WARNING" if health == "GOOD" else health
    reasons.append(f"Invalid events count: {invalid_count}")

seen = set()
duplicates = 0
for e in events:
    sig = (e.get("provider"), e.get("timestamp"), e.get("object_id"))
    if sig in seen:
        duplicates += 1
    seen.add(sig)

if duplicates > len(events) * 0.1: 
    health = "CRITICAL"
    reasons.append(f"High duplicate rate: {duplicates} duplicates")
elif duplicates > 0:
    health = "WARNING" if health == "GOOD" else health
    reasons.append(f"Duplicates detected: {duplicates}")

alert_count = sum(1 for e in events if float(e.get("temperature", 0)) > 28.0 or float(e.get("velocity", 0)) > 10.0)
if alert_count > 5:
    health = "CRITICAL"
    reasons.append(f"Multiple severe alerts detected: {alert_count}")
elif alert_count > 0:
    health = "WARNING" if health == "GOOD" else health
    reasons.append(f"Threshold alerts detected: {alert_count}")

print(f"STREAM HEALTH: {health}")
if reasons:
    print("\nReasons:")
    for r in reasons:
        print(f"- {r}")

