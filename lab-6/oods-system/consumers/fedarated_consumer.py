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

while True:
    try:
        response = requests.get(BROKER_EVENTS_URL)
        events = response.json()
    except Exception as e:
        print("Waiting for broker...")
        time.sleep(3)
        continue
        
    print(f"CURRENT NUMBER OF EVENTS: {len(events)}")
    
    per_provider = {}
    valid_count = 0
    invalid_count = 0
    
    for event in events:
        missing = [field for field in required_fields if field not in event]
        if missing:
            print("INVALID EVENT DETECTED:", event)
            print("Missing fields:", missing)
            invalid_count += 1
            continue 
        else:
            valid_count += 1
            
        provider = event.get("provider", "unknown")
        per_provider[provider] = per_provider.get(provider, 0) + 1
        
    print(f"VALID EVENTS: {valid_count}")
    print(f"INVALID EVENTS: {invalid_count}")

    print("PER PROVIDER:")
    for k, v in per_provider.items():
        print(f"{k}: {v}")

    objects = sorted(set(event.get("object_id") for event in events if "object_id" in event))
    print(f"DISTINCT OBJECTS: {len(objects)}")
    
    selected_object = "OBJ-003"
    selected_count = sum(1 for event in events if event.get("object_id") == selected_object)
    print(f"{selected_object} OBSERVATIONS: {selected_count}")

    active = sorted(set(event.get("provider") for event in events if "provider" in event))
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
