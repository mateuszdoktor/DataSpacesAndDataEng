import os
import numpy as np
import rasterio
import matplotlib.pyplot as plt
from rasterio.transform import from_origin

OUTPUT_DIR="results/ndvi_timeseries"
REPORT_FILE="reports/time_series_ndvi_monitoring.txt"

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)
os.makedirs(
    "reports",
    exist_ok=True
)

def create_sample_observation(name, vegetation_level):
    width=300
    height=300
    transform=from_origin(19.0, 51.0, 10, 10)

    red=np.random.normal(1200, 250, (height, width)).astype("float32")
    nir=np.random.normal(vegetation_level, 400, (height, width)).astype("float32")

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
    return (nir-red)/(nir+red+1e-6)

def save_ndvi_map(ndvi, output_path, title):
    plt.figure(figsize=(6,6))
    plt.imshow(ndvi, cmap="RdYlGn", vmin=-1, vmax=1)
    plt.colorbar()
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

observations = [
    ("observation_2024_04_01", 2600),
    ("observation_2024_05_01", 3400),
    ("observation_2024_06_01", 4300)
]

dates = ["2024-04-01", "2024-05-01", "2024-06-01"]
mean_values = []
report_lines = []

for name, vegetation_level in observations:
    print(
        "Processing:",
        name
    )
    red_path, nir_path = create_sample_observation(name, vegetation_level)
    ndvi = compute_ndvi(red_path, nir_path)
    mean_ndvi = float(np.mean(ndvi))
    mean_values.append(mean_ndvi)

    report_lines.append(name)
    report_lines.append(f"Mean NDVI: {mean_ndvi:.2f}")

    map_path = f"{OUTPUT_DIR}/{name}_ndvi_map.png"
    save_ndvi_map(ndvi, map_path, name)

plt.figure(figsize=(6,4))
plt.plot(dates, mean_values, marker="o")
plt.title("Mean NDVI")
plt.ylabel("Mean NDVI")
plt.tight_layout()
plt.savefig(f"{OUTPUT_DIR}/mean_ndvi_trend.png", dpi=150)
plt.close()

change = mean_values[-1] - mean_values[0]
if change > 0.05:
    trend = "increasing vegetation activity"
elif change < -0.05:
    trend = "decreasing vegetation activity"
else:
    trend = "stable vegetation conditions"

with open(REPORT_FILE, "w") as f:
    f.write("TIME-SERIES NDVI MONITORING REPORT\n")
    f.write("==================================\n")
    f.write("2024-04-01\n")
    f.write(f"Mean NDVI: {mean_values[0]:.2f}\n")
    f.write("2024-05-01\n")
    f.write(f"Mean NDVI: {mean_values[1]:.2f}\n")
    f.write("2024-06-01\n")
    f.write(f"Mean NDVI: {mean_values[2]:.2f}\n")
    f.write("Vegetation trend:\n")
    f.write("-----------------\n")
    f.write(f"First mean NDVI: {mean_values[0]:.2f}\n")
    f.write(f"Last mean NDVI: {mean_values[2]:.2f}\n")
    f.write(f"Change: {change:+.2f}\n")
    f.write("Detected trend:\n")
    f.write(trend.title() + "\n")

print("TIME-SERIES NDVI MONITORING COMPLETE")
print("Generated files:")
print("results/ndvi_timeseries/")
print("observation_2024_04_01_ndvi_map.png")
print("observation_2024_05_01_ndvi_map.png")
print("observation_2024_06_01_ndvi_map.png")
print("mean_ndvi_trend.png")
print("reports/")
print("time_series_ndvi_monitoring.txt")
