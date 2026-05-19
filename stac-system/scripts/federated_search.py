import json
import os
from datetime import datetime

import requests

BASE_URL = "https://catalogue.dataspace.copernicus.eu/stac"
SEARCH_URL = f"{BASE_URL}/search"
COLLECTIONS_URL = f"{BASE_URL}/collections"


def fetch_collections():
    try:
        response = requests.get(COLLECTIONS_URL, timeout=60)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException:
        return set()
    return {entry.get("id") for entry in data.get("collections", [])}


def run_query(name, collection, bbox, time_range, limit=20):
    payload = {
        "collections": [collection],
        "bbox": bbox,
        "datetime": time_range,
        "limit": limit,
    }
    response = requests.post(SEARCH_URL, json=payload, timeout=60)
    response.raise_for_status()
    data = response.json()
    items = data.get("features", [])
    return items


def normalize_item(item, source_query):
    assets = item.get("assets", {})
    return {
        "id": item.get("id"),
        "collection": item.get("collection"),
        "datetime": item.get("properties", {}).get("datetime"),
        "bbox": item.get("bbox"),
        "assets_count": len(assets) if isinstance(assets, dict) else 0,
        "source_provider": "CDSE",
        "source_query": source_query,
    }


def sort_key(entry):
    value = entry.get("datetime")
    if not value:
        return ""
    return value


def main():
    os.makedirs("results", exist_ok=True)
    os.makedirs("reports", exist_ok=True)

    available = fetch_collections()
    radar_collection = "sentinel-1-grd"
    if radar_collection not in available:
        radar_collection = "ccm-sar"

    queries = [
        {
            "name": "optical_southern_poland",
            "collection": "sentinel-2-l2a",
            "bbox": [19.0, 50.0, 20.0, 51.0],
            "time": "2024-01-01T00:00:00Z/2024-01-10T23:59:59Z",
        },
        {
            "name": "optical_baltic_region",
            "collection": "sentinel-2-l2a",
            "bbox": [17.0, 54.0, 19.0, 55.5],
            "time": "2024-01-01T00:00:00Z/2024-01-10T23:59:59Z",
        },
        {
            "name": "radar_southern_poland",
            "collection": radar_collection,
            "bbox": [19.0, 50.0, 20.0, 51.0],
            "time": "2024-01-01T00:00:00Z/2024-01-10T23:59:59Z",
        },
    ]

    raw_items = []
    normalized = []
    ids_seen = set()
    duplicates = 0
    failures = 0
    empty_queries = 0
    per_query_counts = {}

    for query in queries:
        name = query["name"]
        collection = query["collection"]
        try:
            items = run_query(name, collection, query["bbox"], query["time"], limit=20)
        except requests.exceptions.RequestException as exc:
            failures += 1
            per_query_counts[name] = 0
            continue

        per_query_counts[name] = len(items)
        if not items:
            empty_queries += 1

        for item in items:
            raw_items.append({"source_query": name, "item": item})
            item_id = item.get("id")
            if item_id in ids_seen:
                duplicates += 1
                continue
            ids_seen.add(item_id)
            normalized.append(normalize_item(item, name))

    normalized.sort(key=sort_key)

    with open("results/raw_stac_items.json", "w", encoding="utf-8") as handle:
        json.dump(raw_items, handle, indent=2)

    with open("results/federated_results.json", "w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2)

    datetimes = [entry.get("datetime") for entry in normalized if entry.get("datetime")]
    earliest = min(datetimes) if datetimes else "N/A"
    latest = max(datetimes) if datetimes else "N/A"
    collections = sorted({entry.get("collection") for entry in normalized if entry.get("collection")})

    summary_lines = [
        "FEDERATED SEARCH SUMMARY",
        "========================",
        "",
        f"Radar collection used: {radar_collection}",
        "",
        "Products per query:",
    ]
    for name, count in per_query_counts.items():
        summary_lines.append(f"- {name}: {count}")

    radar_count = per_query_counts.get("radar_southern_poland", 0)
    optical_total = per_query_counts.get("optical_southern_poland", 0) + per_query_counts.get(
        "optical_baltic_region", 0
    )
    interpretation = ["INTERPRETATION:"]
    if radar_count == 0:
        interpretation.append("- Radar query returned 0 items; consider another SAR collection")
        interpretation.append("- Optical collections provide current coverage in this time window")
    else:
        interpretation.append("- Radar adds all-weather coverage for cloudy periods")
        interpretation.append("- Optical collections provide richer spectral detail")
    if optical_total == 0:
        interpretation.append("- No optical products found; check bbox or time window")

    summary_lines.extend(
        [
            "",
            f"DUPLICATES_REMOVED: {duplicates}",
            f"TOTAL_UNIQUE_PRODUCTS: {len(normalized)}",
            f"EARLIEST_OBSERVATION: {earliest}",
            f"LATEST_OBSERVATION: {latest}",
            f"COLLECTIONS: {', '.join(collections) if collections else 'N/A'}",
            f"FAILED_QUERIES: {failures}",
            f"EMPTY_SEARCH_REGIONS: {empty_queries}",
            "",
        ]
        + interpretation
    )

    with open("reports/federation_summary.txt", "w", encoding="utf-8") as handle:
        handle.write("\n".join(summary_lines))


if __name__ == "__main__":
    main()
