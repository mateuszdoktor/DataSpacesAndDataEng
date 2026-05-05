import requests
import time
import json
import os

BROKER_EVENTS_URL = "http://127.0.0.1:7000/events"

base_path = ".." if os.path.basename(os.getcwd()) == "consumers" else "."
event_schema_path = os.path.join(base_path, "contracts/event_schema.json")
providers_registry_path = os.path.join(base_path, "contracts/providers_registry.json")

with open(event_schema_path, "r") as f:
    contract = json.load(f)
required_fields = contract.get("required_fields", [])

with open(providers_registry_path, "r") as f:
    expected = [p["name"] for p in json.load(f)]

seen_alerts = set()
seen_events = set()

while True:
    try:
        response = requests.get(BROKER_EVENTS_URL)
        events = response.json()
    except Exception as e:
        print("Waiting for broker...")
        time.sleep(3)
        continue
        
    print(f"CURRENT NUMBER OF TOTAL EVENTS: {len(events)}")
    
    WINDOW_SIZE = 10
    recent_events = events[-WINDOW_SIZE:] if len(events) >= WINDOW_SIZE else events
    print(f"--- ANALYZING SLIDING WINDOW (LAST {len(recent_events)} EVENTS) ---")
    
    per_provider = {}
    valid_count = 0
    invalid_count = 0
    
    for event in recent_events:
        missing = [field for field in required_fields if field not in event]
        if missing:
            invalid_count += 1
            continue
        else:
            valid_count += 1
            
        provider = event.get("provider")
        timestamp = event.get("timestamp")
        object_id = event.get("object_id")
        temp = float(event.get("temperature", 0))
        vel = float(event.get("velocity", 0))

        event_sig = (provider, timestamp, object_id)

        if event_sig not in seen_events:
            seen_events.add(event_sig)
            if events.count(event) > 1:
                print("DUPLICATE EVENT DETECTED:")
                print(f"provider={provider}")
                print(f"timestamp={timestamp}")
                print(f"object_id={object_id}")

        per_provider[provider] = per_provider.get(provider, 0) + 1
        
        alert_obj_sig = ("OBJ_ALERT", object_id, timestamp)
        if object_id == "OBJ-003" and alert_obj_sig not in seen_alerts:
            print(f"ALERT: {object_id} observed by {provider} at {timestamp}")
            seen_alerts.add(alert_obj_sig)

        alert_thresh_sig = ("THRESH_ALERT", provider, object_id, timestamp)
        if (temp > 28.0 or vel > 10.0) and alert_thresh_sig not in seen_alerts:
            print(f"WARNING:\nProvider: {provider}\nObject: {object_id}\nTemperature: {temp}\nVelocity: {vel}")
            seen_alerts.add(alert_thresh_sig)

    print(f"VALID EVENTS: {valid_count}")
    print(f"INVALID EVENTS: {invalid_count}")

    print("PER PROVIDER:")
    for k, v in per_provider.items():
        print(f"{k}: {v}")

    objects = sorted(set(event.get("object_id") for event in recent_events if "object_id" in event))
    print(f"DISTINCT OBJECTS: {len(objects)}")
    
    selected_object = "OBJ-003"
    selected_count = sum(1 for event in recent_events if event.get("object_id") == selected_object)
    print(f"{selected_object} OBSERVATIONS: {selected_count}")

    active = sorted(set(event.get("provider") for event in recent_events if "provider" in event))
    missing_provs = sorted(set(expected) - set(active))
    
    print("EXPECTED PROVIDERS:")
    for p in expected: print(p)
    print("ACTIVE PROVIDERS:")
    for p in active: print(p)
    if missing_provs:
        print("MISSING PROVIDERS:")
        for p in missing_provs: print(p)
    else:
        print("MISSING PROVIDERS:")
        print("none")
        
    COMPLETE = "NO" if missing_provs else "YES"
    print(f"COMPLETE: {COMPLETE}")
    print("-" * 40)
    
    time.sleep(3)
