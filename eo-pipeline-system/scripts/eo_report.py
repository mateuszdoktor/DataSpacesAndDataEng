import os
import numpy as np
import rasterio

REPORT_FILE = "reports/eo_processing_report.txt"

os.makedirs(
    "reports",
    exist_ok=True
)

def file_status(path):
    return "available" if os.path.exists(path) else "missing"

def inspect_raster(path):
    if not os.path.exists(path):
        return None
    with rasterio.open(path) as src:
        return {
            "width": src.width,
            "height": src.height,
            "count": src.count,
            "crs": str(src.crs),
            "bounds": str(src.bounds)
        }

def compute_band_statistics(path):
    if not os.path.exists(path):
        return None
    with rasterio.open(path) as src:
        band = src.read(1)
    return {
        "min": float(np.min(band)),
        "max": float(np.max(band)),
        "mean": float(np.mean(band)),
        "std": float(np.std(band))
    }

def compute_ndvi_statistics(path):
    if not os.path.exists(path):
        return None
    ndvi = np.load(path)
    return {
        "min": float(np.min(ndvi)),
        "max": float(np.max(ndvi)),
        "mean": float(np.mean(ndvi)),
        "high_veg": int(np.sum(ndvi > 0.5)),
        "low_ndvi": int(np.sum(ndvi < 0))
    }

def read_ranking_summary(path):
    if not os.path.exists(path):
        return "Ranking report missing."
    with open(path) as f:
        lines = f.readlines()
    return "".join(lines[:20]).strip()

selected_observation = "Selected observation not available."
if os.path.exists("reports/task1_selected_observation.txt"):
    with open("reports/task1_selected_observation.txt") as f:
        selected_observation = f.read().strip()

assets = {
    "thumbnail": "assets/thumbnails/thumbnail.jpg",
    "B04": "assets/bands/B04_10m.tif",
    "B08": "assets/bands/B08_10m.tif",
    "NDVI": "results/ndvi/ndvi.npy",
    "NDVI map": "results/ndvi/ndvi_map.png"
}

raster_info = inspect_raster("assets/bands/B04_10m.tif")
spectral_stats = compute_band_statistics("assets/bands/B04_10m.tif")
ndvi_stats = compute_ndvi_statistics("results/ndvi/ndvi.npy")
ranking_summary = read_ranking_summary("reports/observation_ranking.txt")

vegetation_assessment = "Vegetation assessment not available."
if ndvi_stats:
    if ndvi_stats["mean"] > 0.3:
        vegetation_assessment = "High vegetation coverage detected."
    else:
        vegetation_assessment = "Low vegetation coverage detected."

with open(
    REPORT_FILE,
    "w"
) as f:
    f.write("EO PROCESSING REPORT\n")
    f.write("====================\n")
    f.write("Selected Observation:\n")
    f.write(selected_observation + "\n\n")

    f.write("Assets:\n")
    for name, path in assets.items():
        f.write(f"{name} {file_status(path)}\n")
    f.write("\n")

    f.write("Raster Metadata:\n")
    if raster_info:
        f.write(f"Width: {raster_info['width']}\n")
        f.write(f"Height: {raster_info['height']}\n")
        f.write(f"Bands: {raster_info['count']}\n")
        f.write(f"CRS: {raster_info['crs']}\n")
        f.write(f"Bounds: {raster_info['bounds']}\n")
    else:
        f.write("Raster metadata not available.\n")
    f.write("\n")

    f.write("Spectral Statistics:\n")
    if spectral_stats:
        f.write(f"MIN: {spectral_stats['min']}\n")
        f.write(f"MAX: {spectral_stats['max']}\n")
        f.write(f"MEAN: {spectral_stats['mean']}\n")
        f.write(f"STD: {spectral_stats['std']}\n")
    else:
        f.write("Spectral statistics not available.\n")
    f.write("\n")

    f.write("NDVI Statistics:\n")
    if ndvi_stats:
        f.write(f"MIN: {ndvi_stats['min']}\n")
        f.write(f"MAX: {ndvi_stats['max']}\n")
        f.write(f"MEAN: {ndvi_stats['mean']}\n")
        f.write(f"HIGH VEGETATION PIXELS: {ndvi_stats['high_veg']}\n")
        f.write(f"LOW NDVI PIXELS: {ndvi_stats['low_ndvi']}\n")
    else:
        f.write("NDVI statistics not available.\n")
    f.write("\n")

    f.write("Vegetation Assessment:\n")
    f.write(vegetation_assessment + "\n\n")

    f.write("Observation Ranking:\n")
    f.write(ranking_summary + "\n")

print(
    "REPORT CREATED"
)
