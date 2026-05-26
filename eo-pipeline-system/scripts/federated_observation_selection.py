import os
import requests

STAC_URL = "https://stac.dataspace.copernicus.eu/v1/search"
AOI = [19.0, 50.0, 20.0, 51.0]
TIME_WINDOW = (
    "2024-01-01T00:00:00Z/"
    "2024-01-31T23:59:59Z"
)
REPORT_FILE = "reports/federated_observation_selection.txt"

QUERIES = {
    "Sentinel-2 Optical": {
        "collections": ["sentinel-2-l2a"],
        "bbox": AOI,
        "datetime": TIME_WINDOW,
        "limit": 5
    },
    "Sentinel-1 SAR": {
        "collections": ["sentinel-1-grd"],
        "bbox": AOI,
        "datetime": TIME_WINDOW,
        "limit": 5
    }
}

scenarios = [
    "normal",
    "cloudy",
    "night"
]

def query_stac(query):
    response = requests.post(
        STAC_URL,
        json=query,
        timeout=30
    )
    response.raise_for_status()
    return response.json().get("features", [])

def summarize_item(item):
    properties = item.get("properties", {})
    assets = item.get("assets", {})
    return {
        "id": item.get("id"),
        "datetime": properties.get("datetime"),
        "cloud_cover": properties.get("eo:cloud_cover"),
        "assets_count": len(assets),
        "platform": properties.get("platform"),
        "constellation": properties.get("constellation"),
        "instrument": properties.get("instruments")
    }

def compute_sensor_score(sensor_name, items, scenario):
    if not items:
        return 0
    score = 0
    score += min(len(items), 5) * 10
    
    if sensor_name == "Sentinel-2 Optical":
        if scenario == "normal":
            avg_cloud_cover = sum(float(item.get("cloud_cover") or 0) for item in items) / len(items)
            score += (100 - avg_cloud_cover)
        if scenario == "cloudy":
            score += 18
        if scenario == "night":
            score -= 10
            
    if sensor_name == "Sentinel-1 SAR":
        if scenario == "normal":
            score += 50
        if scenario == "cloudy":
            score += 80
        if scenario == "night":
            score += 80
            
    return score

def select_best_sensor(summaries, scenario):
    scores = {}
    for sensor_name, items in summaries.items():
        scores[sensor_name] = compute_sensor_score(sensor_name, items, scenario)
    
    best_sensor = max(scores, key=scores.get)
    return best_sensor, scores

def main():
    print("FEDERATED OBSERVATION SELECTION")
    print("============================================================")
    
    os.makedirs("reports", exist_ok=True)
    
    summaries = {}
    for sensor_name, query in QUERIES.items():
        print(f"Querying: {sensor_name}")
        features = query_stac(query)
        print(f"{sensor_name} products: {len(features)}")
        summaries[sensor_name] = [summarize_item(f) for f in features]

    report_lines = [
        "FEDERATED OBSERVATION SELECTION REPORT",
        "======================================",
        "Sensors compared:",
        "- Sentinel-2 Optical",
        "- Sentinel-1 SAR",
        "Scenario-based sensor selection:",
        "-------------------------------"
    ]
    
    for scenario in scenarios:
        best_sensor, scores = select_best_sensor(summaries, scenario)
        report_lines.append(f"Scenario: {scenario}")
        report_lines.append(f"Recommended sensor: {best_sensor}")
        report_lines.append("Scores:")
        report_lines.append(f"- Sentinel-2 Optical: {scores.get('Sentinel-2 Optical', 0):.2f}")
        report_lines.append(f"- Sentinel-1 SAR: {scores.get('Sentinel-1 SAR', 0):.2f}")
        
    with open(REPORT_FILE, "w") as f:
        f.write("\n".join(report_lines) + "\n")
        
    print("FEDERATED SELECTION COMPLETE")
    print(f"REPORT SAVED TO: {REPORT_FILE}")

if __name__ == "__main__":
    main()
