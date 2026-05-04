import json
import requests
with open("../contracts/providers_registry.json", "r") as f:
    providers = json.load(f)
all_results = []
for provider in providers:
    provider_name = provider["name"]
    provider_url = provider["url"]
    
    response = requests.get(f"{provider_url}/observations")
    data = response.json()

    for row in data:
        row["provider"] = provider_name
        all_results.append(row)

print("TOTAL RECORDS:", len(all_results))

sat_A_count = 0
sat_B_count = 0
ground_stat_count = 0

for row in all_results:
    #print(row.keys())
    if (row['provider'] == 'satellite_A'):
        sat_A_count += 1
    elif(row['provider'] == 'satellite_B'):
        sat_B_count += 1
    elif(row['provider'] == 'ground_station'):
        ground_stat_count += 1
    
    #print(row)

print(f"PER-PROVIDER COUNTS: satelite_A={sat_A_count}, satellite_B={sat_B_count}, ground_station={ground_stat_count}")

print("LARGEST PROVIDER")
counts = [sat_A_count,sat_B_count,ground_stat_count]
max_count = max(counts)
max_idx = (counts.index(max_count))

if (max_idx == 0):
        print("satellite_A")
elif(max_idx == 1):
        print("satellite_B")
elif(max_idx == 2):
        print("ground_station")

