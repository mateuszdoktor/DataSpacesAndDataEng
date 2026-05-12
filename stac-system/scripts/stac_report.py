import json
import os
from datetime import datetime


def parse_summary(path):
    values = {
        "duplicates": "N/A",
        "failed_queries": "N/A",
        "empty_regions": "N/A",
    }
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line.startswith("DUPLICATES_REMOVED:"):
                values["duplicates"] = line.split(":", 1)[1].strip()
            elif line.startswith("FAILED_QUERIES:"):
                values["failed_queries"] = line.split(":", 1)[1].strip()
            elif line.startswith("EMPTY_SEARCH_REGIONS:"):
                values["empty_regions"] = line.split(":", 1)[1].strip()
    return values


def compute_temporal_coverage(items):
    datetimes = [entry.get("datetime") for entry in items if entry.get("datetime")]
    if not datetimes:
        return "N/A", "N/A", "N/A"
    earliest = min(datetimes)
    latest = max(datetimes)
    try:
        start = datetime.fromisoformat(earliest.replace("Z", "+00:00"))
        end = datetime.fromisoformat(latest.replace("Z", "+00:00"))
        span = end - start
        span_str = f"{span.days} days"
    except ValueError:
        span_str = "N/A"
    return earliest, latest, span_str


def compute_spatial_coverage(items):
    bboxes = [entry.get("bbox") for entry in items if entry.get("bbox")]
    if not bboxes:
        return "N/A"
    min_lon = min(b[0] for b in bboxes)
    min_lat = min(b[1] for b in bboxes)
    max_lon = max(b[2] for b in bboxes)
    max_lat = max(b[3] for b in bboxes)
    return f"[{min_lon}, {min_lat}, {max_lon}, {max_lat}]"


def compute_asset_availability(raw_items):
    asset_keys = set()
    for entry in raw_items:
        item = entry.get("item", {})
        assets = item.get("assets", {})
        if isinstance(assets, dict):
            asset_keys.update(assets.keys())
    if not asset_keys:
        return "N/A"
    sample = sorted(asset_keys)[:8]
    return ", ".join(sample)


def metadata_completeness(items):
    required = ["id", "collection", "datetime", "bbox", "assets_count"]
    if not items:
        return "N/A"
    complete = 0
    for entry in items:
        if all(entry.get(field) is not None for field in required):
            complete += 1
    score = complete / len(items) * 100
    return f"{score:.1f}%"


def main():
    os.makedirs("reports", exist_ok=True)
    with open("results/federated_results.json", "r", encoding="utf-8") as handle:
        items = json.load(handle)

    raw_items = []
    if os.path.exists("results/raw_stac_items.json"):
        with open("results/raw_stac_items.json", "r", encoding="utf-8") as handle:
            raw_items = json.load(handle)

    summary_values = parse_summary("reports/federation_summary.txt")
    collections = sorted({entry.get("collection") for entry in items if entry.get("collection")})
    earliest, latest, span = compute_temporal_coverage(items)
    spatial = compute_spatial_coverage(items)
    assets = compute_asset_availability(raw_items)
    completeness = metadata_completeness(items)

    lines = [
        "STAC REPORT",
        "==========",
        "",
        f"Total items: {len(items)}",
        f"Collections: {', '.join(collections) if collections else 'N/A'}",
        f"Temporal coverage: {earliest} to {latest} (span {span})",
        f"Spatial coverage: {spatial}",
        f"Duplicate items removed: {summary_values['duplicates']}",
        f"Failed queries: {summary_values['failed_queries']}",
        f"Empty search regions: {summary_values['empty_regions']}",
        f"Assets available (sample): {assets}",
        f"Metadata completeness score: {completeness}",
    ]

    with open("reports/stac_report.txt", "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


if __name__ == "__main__":
    main()
