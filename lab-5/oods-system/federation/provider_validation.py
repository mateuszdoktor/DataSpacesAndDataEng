import json
import requests
import os

with open(os.path.join(os.path.dirname(__file__), "../contracts/providers_registry.json"), "r") as f:
    providers = json.load(f)

with open(os.path.join(os.path.dirname(__file__), "../contracts/observation_schema.json"), "r") as f:
    contract = json.load(f)

required_fields = contract["required_fields"]

print("=== PROVIDER VALIDATION ===")

status_counts = {"OK": 0, "VIOLATION": 0, "UNAVAILABLE": 0, "EMPTY DATASET": 0}
problematic = []

for provider in providers:
    provider_name = provider["name"]
    provider_url = provider["url"]
    
    try:
        response = requests.get(f"{provider_url}/observations", timeout=2)
        data = response.json()
        
        if len(data) == 0:
            print(provider_name, ": EMPTY DATASET")
            status_counts["EMPTY DATASET"] += 1
            problematic.append(provider_name)
            continue
            
        missing = [field for field in required_fields if field not in data[0]]
        
        if missing:
            print(provider_name, ": VIOLATION")
            print("Missing fields:", missing)
            status_counts["VIOLATION"] += 1
            problematic.append(provider_name)
        else:
            print(provider_name, ": OK")
            status_counts["OK"] += 1

    except Exception:
        print(provider_name, ": UNAVAILABLE")
        status_counts["UNAVAILABLE"] += 1
        problematic.append(provider_name)

print("\n=== VALIDATION SUMMARY ===")
for st, count in status_counts.items():
    print(f"{st}: {count}")

print(f"\nProblematic providers: {', '.join(problematic) if problematic else 'None'}")

if len(problematic) == 0:
    print("Federation State: RELIABLE")
else:
    print("Federation State: DEGRADED")
