import os
import requests
STAC_URL="https://stac.dataspace.copernicus.eu/v1/search"
QUERY={
    "collections":["sentinel-2-l2a"],
    "limit":1
}
OUTPUT_PATHS={
"thumbnail":"assets/thumbnails/thumbnail.jpg",
"TCI_10m":"assets/visual/visual.jp2",
"B04_10m":"assets/bands/B04.jp2",
"B08_10m":"assets/bands/B08.jp2"
}
def is_http_url(url):
    return (
    url.startswith("http://")
    or
    url.startswith("https://")
    )
def download_file(url, output_path):
    if not is_http_url(url):
        print(
        "SKIPPED NON-HTTP ASSET:",
        url
        )
        return False
    
    response=requests.get(
        url,
        timeout=120
    )

    response.raise_for_status()

    with open(output_path,"wb") as f:
        f.write(response.content)

    return True

response=requests.post(
    STAC_URL,
    json=QUERY
)

data=response.json()

item=data["features"][0]

assets=item["assets"]

print(
    "AVAILABLE ASSETS:"
)
for asset_name in assets:
    print(
    asset_name
    )

downloaded_count=0
skipped_count=0
failed_count=0

print(
    "\nDOWNLOADING SELECTED ASSETS:"
)

for asset_name, output_path in OUTPUT_PATHS.items():
    if asset_name not in assets:
        print(
            f"SKIPPED - NOT AVAILABLE: {asset_name}"
        )
        skipped_count+=1
        continue
    
    asset_url=assets[asset_name]["href"]
    
    os.makedirs(
        os.path.dirname(output_path),
        exist_ok=True
    )
    
    try:
        if download_file(asset_url, output_path):
            print(
                f"DOWNLOADED: {asset_name} → {output_path}"
            )
            downloaded_count+=1
        else:
            print(
                f"SKIPPED: {asset_name} (non-HTTP URL)"
            )
            skipped_count+=1
    except Exception as e:
        print(
            f"FAILED: {asset_name} - {str(e)}"
        )
        failed_count+=1

print(
    "\n" + "="*50
)
print(
    "DOWNLOAD REPORT"
)
print(
    "="*50
)
print(
    f"Downloaded: {downloaded_count}"
)
print(
    f"Skipped: {skipped_count}"
)
print(
    f"Failed: {failed_count}"
)
print(
    f"Total processed: {len(OUTPUT_PATHS)}"
)
print(
    "="*50
)