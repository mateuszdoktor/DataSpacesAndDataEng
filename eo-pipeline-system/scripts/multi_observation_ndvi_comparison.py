import os
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from rasterio.transform import from_origin

OUTPUT_DIR="results/ndvi_comparison"
REPORT_FILE="reports/multi_observation_ndvi_comparison.txt"

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)
os.makedirs(
    "reports",
    exist_ok=True
)

def create_sample_observation(name, cloud_level):
    width=300
    height=300
    transform=from_origin(19.0, 51.0, 10, 10)

    red=np.random.normal(1200, 250, (height, width)).astype("float32")
    nir=np.random.normal(2500, 500, (height, width)).astype("float32")

    if cloud_level > 0:
        cloud_size=int(width * 0.3)
        red[:cloud_size, :cloud_size] = 3000
        nir[:cloud_size, :cloud_size] = 1200

    profile={
        "driver":"GTiff",
        "height":height,
        "width":width,
        "count":1,
        "dtype":"float32",
        "crs":"EPSG:4326",
        "transform":transform
    }

    red_path=f"{OUTPUT_DIR}/{name}_red.tif"
    nir_path=f"{OUTPUT_DIR}/{name}_nir.tif"

    with rasterio.open(red_path, "w", **profile) as dst:
        dst.write(red, 1)
    with rasterio.open(nir_path, "w", **profile) as dst:
        dst.write(nir, 1)

    return red_path, nir_path

def compute_ndvi(red_path, nir_path):
    with rasterio.open(red_path) as red_src:
        red=red_src.read(1).astype(float)
    with rasterio.open(nir_path) as nir_src:
        nir=nir_src.read(1).astype(float)
    ndvi=(nir-red)/(nir+red+1e-6)
    return ndvi

def save_ndvi_map(ndvi, output_path, title):
    plt.figure(figsize=(6,6))
    plt.imshow(ndvi, cmap="RdYlGn", vmin=-1, vmax=1)
    plt.colorbar()
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

low_red, low_nir = create_sample_observation("low_cloud_observation", cloud_level=0.05)
high_red, high_nir = create_sample_observation("high_cloud_observation", cloud_level=0.75)

low_ndvi = compute_ndvi(low_red, low_nir)
high_ndvi = compute_ndvi(high_red, high_nir)

low_map = f"{OUTPUT_DIR}/low_cloud_observation_ndvi_map.png"
high_map = f"{OUTPUT_DIR}/high_cloud_observation_ndvi_map.png"

save_ndvi_map(low_ndvi, low_map, "Low Cloud NDVI")
save_ndvi_map(high_ndvi, high_map, "High Cloud NDVI")

low_stats = {
    "min": float(low_ndvi.min()),
    "max": float(low_ndvi.max()),
    "mean": float(low_ndvi.mean()),
    "high": int((low_ndvi > 0.5).sum()),
    "low": int((low_ndvi < 0).sum())
}

high_stats = {
    "min": float(high_ndvi.min()),
    "max": float(high_ndvi.max()),
    "mean": float(high_ndvi.mean()),
    "high": int((high_ndvi > 0.5).sum()),
    "low": int((high_ndvi < 0).sum())
}

with open(REPORT_FILE, "w") as f:
    f.write("MULTI-OBSERVATION NDVI COMPARISON\n")
    f.write("=================================\n")
    f.write("low_cloud_observation\n")
    f.write("--------------------\n")
    f.write("Cloud cover: 5%\n")
    f.write(f"NDVI min: {low_stats['min']:.2f}\n")
    f.write(f"NDVI max: {low_stats['max']:.2f}\n")
    f.write(f"NDVI mean: {low_stats['mean']:.2f}\n")
    f.write(f"High vegetation pixels: {low_stats['high']}\n")
    f.write(f"Low NDVI pixels: {low_stats['low']}\n")
    f.write("high_cloud_observation\n")
    f.write("---------------------\n")
    f.write("Cloud cover: 75%\n")
    f.write(f"NDVI min: {high_stats['min']:.2f}\n")
    f.write(f"NDVI max: {high_stats['max']:.2f}\n")
    f.write(f"NDVI mean: {high_stats['mean']:.2f}\n")
    f.write(f"High vegetation pixels: {high_stats['high']}\n")
    f.write(f"Low NDVI pixels: {high_stats['low']}\n")
    f.write("ENGINEERING COMPARISON\n")
    f.write("---------------------\n")
    f.write("The low-cloud observation provides a clearer and more useful NDVI result.\n")
    f.write("The high-cloud observation is operationally less reliable because clouds\n")
    f.write("reduce interpretability and may hide the real surface signal.\n")
    f.write("Recommended observation:\n")
    f.write("low_cloud_observation\n")

print("COMPARISON COMPLETE")
print("Generated files:")
print("- results/ndvi_comparison/low_cloud_observation_ndvi_map.png")
print("- results/ndvi_comparison/high_cloud_observation_ndvi_map.png")
print("- reports/multi_observation_ndvi_comparison.txt")
