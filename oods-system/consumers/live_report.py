import requests
import json
import os
import datetime

BROKER_EVENTS_URL = "http://127.0.0.1:7000/events"

base_path = ".." if os.path.basename(os.getcwd()) == "consumers" else "."
event_schema_path = os.path.join(base_path, "contracts/event_schema.json")
providers_registry_path = os.path.join(base_path, "contracts/providers_registry.json")
reports_dir = os.path.join(base_path, "reports")

if not os.path.exists(reports_dir):
    os.makedirs(reports_dir)

try:
    with open(event_schema_path, "r") as f:
        contract = json.load(f)
    required_fields = contract.get("required_fields", [])
except:
    required_fields = []

try:
    with open(providers_registry_path, "r") as f:
        expected = [p["name"] for p in json.load(f)]
except:
    expected = []

try:
    response = requests.get(BROKER_EVENTS_URL)
    events = response.json()
except Exception as e:
    print("Could not fetch events from broker:", e)
    events = []

per_provider = {}
valid_count = 0
invalid_count = 0

for event in events:
    missing = [field for field in required_fields if field not in event]
    if missing:
        invalid_count += 1
        continue
    else:
        valid_count += 1
        
    provider = event.get("provider", "unknown")
    per_provider[provider] = per_provider.get(provider, 0) + 1

objects = sorted(set(event.get("object_id") for event in events if "object_id" in event))
selected_object = "OBJ-003"
selected_count = sum(1 for event in events if event.get("object_id") == selected_object)

active = sorted(set(event.get("provider") for event in events if "provider" in event))
missing_provs = sorted(set(expected) - set(active))
COMPLETE = "NO" if missing_provs else "YES"

now = datetime.datetime.now()
timestamp_str = now.strftime("%Y%m%d_%H%M%S")
report_time = now.strftime("%Y-%m-%d %H:%M:%S")

report_filename = os.path.join(reports_dir, f"stream_report_{timestamp_str}.txt")

lines = []
lines.append("REAL-TIME FEDERATION REPORT")
lines.append("-" * 26)
lines.append(f"Generated at: {report_time}")
lines.append("")
lines.append("[STREAM STATUS]")
lines.append(f"Total events: {len(events)}")
lines.append(f"Valid events: {valid_count}")
lines.append(f"Invalid events: {invalid_count}")
lines.append("")
lines.append("[PROVIDERS]")
lines.append("Active providers:")
for p in active:
    lines.append(f"- {p}")
if not active: lines.append("- none")

lines.append("")
lines.append("Missing providers:")
if missing_provs:
    for p in missing_provs:
        lines.append(f"- {p}")
else:
    lines.append("- none")

lines.append("")
lines.append("[PER-PROVIDER COUNTS]")
for k, v in per_provider.items():
    lines.append(f"{k}: {v}")

lines.append("")
lines.append("[OBJECT STATISTICS]")
lines.append(f"Distinct objects: {len(objects)}")
lines.append(f"{selected_object} observations: {selected_count}")

lines.append("")
lines.append("[FEDERATION STATUS]")
lines.append(f"COMPLETE: {COMPLETE}")

report_content = "\n".join(lines)

with open(report_filename, "w") as f:
    f.write(report_content)

print(f"Report generated: {report_filename}")
