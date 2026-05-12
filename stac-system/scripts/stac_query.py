import os
import re
import requests

URL = "https://catalogue.dataspace.copernicus.eu/stac/search"
QUERY = {
    "collections": ["sentinel-2-l2a"],
    "limit": 5,
}

SATELLITE_MAP = {
    "S2A": "Sentinel-2A",
    "S2B": "Sentinel-2B",
    "S2C": "Sentinel-2C",
}


def infer_satellite(product_id):
    if not product_id:
        return "Unknown"
    prefix = product_id.split("_")[0]
    return SATELLITE_MAP.get(prefix, prefix or "Unknown")


def infer_tile(product_id):
    if not product_id:
        return "Unknown"
    match = re.search(r"_T([0-9]{2}[A-Z]{3})", product_id)
    if match:
        return match.group(1)
    return "Unknown"


def fetch_items():
    try:
        response = requests.post(URL, json=QUERY, timeout=60)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as exc:
        print("REQUEST FAILED:", exc)
        return []
    except ValueError:
        print("INVALID JSON RESPONSE")
        return []
    return data.get("features", [])


def build_report(items):
    lines = []
    lines.append("PYTHON STAC RESULTS")
    lines.append("===================")
    lines.append("")
    lines.append(f"Total items: {len(items)}")
    lines.append("")

    satellites = set()
    datetimes = set()
    tiles = set()

    for item in items:
        item_id = item.get("id", "")
        datetime_value = item.get("properties", {}).get("datetime", "")
        assets = item.get("assets", {})
        asset_count = len(assets) if isinstance(assets, dict) else 0
        satellite = infer_satellite(item_id)
        tile = infer_tile(item_id)

        satellites.add(satellite)
        if datetime_value:
            datetimes.add(datetime_value)
        tiles.add(tile)

        lines.append("ITEM")
        lines.append(f"ID: {item_id}")
        lines.append(f"TIME: {datetime_value}")
        lines.append(f"SATELLITE: {satellite}")
        lines.append(f"TILE: {tile}")
        lines.append(f"ASSETS: {asset_count}")
        lines.append("")

    lines.append("ANALYSIS")
    lines.append("--------")
    lines.append(f"Products returned: {len(items)}")
    lines.append(f"Unique satellites: {len(satellites)} ({', '.join(sorted(satellites))})")
    lines.append(f"Unique acquisition times: {len(datetimes)}")
    lines.append(f"Unique tiles: {len(tiles)} ({', '.join(sorted(tiles))})")
    lines.append("")
    lines.append("Why Python querying helps:")
    lines.append("- Enables repeatable queries and structured parsing")
    lines.append("- Supports automated metadata extraction and reporting")
    lines.append("- Scales to batch processing across multiple collections")
    return "\n".join(lines)


def main():
    items = fetch_items()
    report = build_report(items)
    os.makedirs("reports", exist_ok=True)
    with open("reports/python_stac_results.txt", "w", encoding="utf-8") as handle:
        handle.write(report)
    print(report)


if __name__ == "__main__":
    main()
