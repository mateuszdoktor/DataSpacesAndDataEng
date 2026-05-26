import os
import requests
from datetime import datetime, timezone

STAC_URL = "https://stac.dataspace.copernicus.eu/v1/search"

QUERY = {
    "collections": ["sentinel-2-l2a"],
    "bbox": [19.0, 50.0, 20.0, 51.0],
    "datetime": "2024-01-01T00:00:00Z/2024-01-31T23:59:59Z",
    "limit": 10
}

REPORT_FILE = "reports/observation_ranking.txt"

os.makedirs(
    "reports",
    exist_ok=True
)

def parse_datetime(value):
    if value is None:
        return None
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)

def compute_cloud_score(cloud_cover):
    if cloud_cover is None:
        return 0
    return max(0.0, 100.0 - float(cloud_cover))

def compute_completeness_score(assets_count):
    if assets_count >= 30:
        return 30.0
    if assets_count >= 20:
        return 20.0
    if assets_count >= 10:
        return 10.0
    return 0.0

def compute_recency_score(acquisition_time, newest_time):
    if acquisition_time is None or newest_time is None:
        return 0.0
    total_days = 30.0
    age_days = (newest_time - acquisition_time).total_seconds() / 86400.0
    score = max(0.0, 20.0 - (age_days / total_days) * 20.0)
    return score

response = requests.post(
    STAC_URL,
    json=QUERY,
    timeout=30
)
response.raise_for_status()

data = response.json()
features = data.get(
    "features",
    []
)

print(
    "Returned observations:",
    len(features)
)

parsed_times = []
for item in features:
    value = item.get("properties", {}).get("datetime")
    parsed = parse_datetime(value)
    if parsed:
        parsed_times.append(parsed)

newest_time = max(parsed_times) if parsed_times else datetime.now(timezone.utc)

ranked = []

for item in features:
    properties = item.get(
        "properties",
        {}
    )
    assets = item.get(
        "assets",
        {}
    )
    product_id = item.get(
        "id"
    )
    acquisition_time = parse_datetime(
        properties.get("datetime")
    )
    cloud_cover = properties.get(
        "eo:cloud_cover"
    )
    assets_count = len(
        assets
    )

    cloud_score = compute_cloud_score(cloud_cover)
    completeness_score = compute_completeness_score(assets_count)
    recency_score = compute_recency_score(acquisition_time, newest_time)
    final_score = cloud_score + completeness_score + recency_score

    ranked.append({
        "product_id": product_id,
        "acquisition_time": properties.get("datetime"),
        "cloud_cover": cloud_cover,
        "assets_count": assets_count,
        "cloud_score": cloud_score,
        "completeness_score": completeness_score,
        "recency_score": recency_score,
        "final_score": final_score
    })

ranked = sorted(
    ranked,
    key=lambda x: x["final_score"],
    reverse=True
)

with open(
    REPORT_FILE,
    "w"
) as f:
    f.write("OBSERVATION RANKING REPORT\n")
    f.write("="*60 + "\n")
    for idx, item in enumerate(ranked, start=1):
        f.write(f"{idx}. {item['product_id']}\n")
        f.write(f"Time: {item['acquisition_time']}\n")
        f.write(f"Cloud cover: {item['cloud_cover']}\n")
        f.write(f"Assets count: {item['assets_count']}\n")
        f.write(f"Cloud score: {item['cloud_score']:.2f}\n")
        f.write(f"Completeness score: {item['completeness_score']:.2f}\n")
        f.write(f"Recency score: {item['recency_score']:.2f}\n")
        f.write(f"Final score: {item['final_score']:.2f}\n")
        f.write("-"*50 + "\n")

    if ranked:
        best = ranked[0]
        f.write("Recommended observation:\n")
        f.write(f"{best['product_id']}\n")
        f.write("Reason: Highest final score based on cloud cover, asset completeness and recency.\n")
        f.write("This simplified ranking may not be sufficient for real missions.\n")

print(
    "OBSERVATION RANKING ENGINE"
)
print(
    "="*60
)
for idx, item in enumerate(ranked, start=1):
    print(
        f"{idx}. {item['product_id']}"
    )
    print(
        "Time:",
        item["acquisition_time"]
    )
    print(
        "Cloud cover:",
        item["cloud_cover"]
    )
    print(
        "Assets count:",
        item["assets_count"]
    )
    print(
        "Cloud score:",
        f"{item['cloud_score']:.2f}"
    )
    print(
        "Completeness score:",
        f"{item['completeness_score']:.2f}"
    )
    print(
        "Recency score:",
        f"{item['recency_score']:.2f}"
    )
    print(
        "Final score:",
        f"{item['final_score']:.2f}"
    )
    print(
        "-"*50
    )

print(
    f"REPORT SAVED TO: {REPORT_FILE}"
)
