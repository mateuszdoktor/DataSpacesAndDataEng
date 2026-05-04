import json
import requests
with open("../contracts/providers_registry.json", "r") as f:
    providers = json.load(f)
object_id = "OBJ-003"
results = []
providers_with_object = []
for provider in providers:
    provider_name = provider["name"]
    provider_url = provider["url"]
    response = requests.get(f"{provider_url}/observations/{object_id}")
    data = response.json()
    if data:
        providers_with_object.append(provider_name)
    for row in data:
        row["provider"] = provider_name
    results.append(row)
print("OBJECT:", object_id)
print("TOTAL OBSERVATIONS:", len(results))
print("PROVIDERS CONTAINING OBJECT:")
for name in providers_with_object:
    print(name)

print("\nMISSING PROVIDERS")
missing_providers = []
if "satellite_A" not in providers_with_object:
    missing_providers.append("satellite_A")
elif "satellite_B" not in providers_with_object:
    missing_providers.append("satellite_B")
elif "ground_station" not in providers_with_object:
    missing_providers.append("ground_station")

if (len(missing_providers) == 0):
    print("none")
else:
    for name in missing_providers:
        print(name)

completness = "NO"
if (len(missing_providers) == 0):
    completness = "YES"
print(f"\nCOMPLETNESS: {completness}")
