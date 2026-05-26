import os
import numpy as np
import matplotlib.pyplot as plt

NDVI_PATH = "results/ndvi/ndvi.npy"
OUTPUT_IMAGE = "results/ndvi/ndvi_map.png"
REPORT_FILE = "reports/ndvi_analysis.txt"

os.makedirs(
    "results/ndvi",
    exist_ok=True
)
os.makedirs(
    "reports",
    exist_ok=True
)

print(
    "NDVI VISUALIZATION"
)
print(
    "="*50
)

if not os.path.exists(NDVI_PATH):
    print(
        f"Missing NDVI file: {NDVI_PATH}"
    )
    raise SystemExit(1)

ndvi = np.load(
    NDVI_PATH
)

ndvi_min = float(ndvi.min())
ndvi_max = float(ndvi.max())
ndvi_mean = float(ndvi.mean())

high_vegetation_pixels = int(
    np.sum(ndvi > 0.5)
)
low_ndvi_pixels = int(
    np.sum(ndvi < 0)
)

plt.imshow(
    ndvi,
    cmap="RdYlGn",
    vmin=-1,
    vmax=1
)
plt.colorbar()
plt.savefig(
    OUTPUT_IMAGE
)
plt.close()

print(
    "NDVI MIN:",
    ndvi_min
)
print(
    "NDVI MAX:",
    ndvi_max
)
print(
    "NDVI MEAN:",
    ndvi_mean
)
print(
    "HIGH VEGETATION PIXELS:",
    high_vegetation_pixels
)
print(
    "LOW NDVI PIXELS:",
    low_ndvi_pixels
)
print(
    f"NDVI MAP SAVED TO: {OUTPUT_IMAGE}"
)

interpretation = (
    "Vegetated areas correspond to higher NDVI values. "
    "Non-vegetated areas correspond to lower or negative NDVI values. "
    "Low NDVI areas may represent water, shadows or urban surfaces. "
    "No direct cloud contamination can be confirmed because the input bands "
    "were synthetically generated for pipeline testing."
)

with open(
    REPORT_FILE,
    "w"
) as f:
    f.write("NDVI ANALYSIS REPORT\n")
    f.write("====================\n")
    f.write(f"Input NDVI file: {NDVI_PATH}\n")
    f.write(f"NDVI map: {OUTPUT_IMAGE}\n")
    f.write(f"NDVI MIN: {ndvi_min}\n")
    f.write(f"NDVI MAX: {ndvi_max}\n")
    f.write(f"NDVI MEAN: {ndvi_mean}\n")
    f.write(f"HIGH VEGETATION PIXELS: {high_vegetation_pixels}\n")
    f.write(f"LOW NDVI PIXELS: {low_ndvi_pixels}\n")
    f.write("Interpretation:\n")
    f.write(interpretation + "\n")

print(
    f"ANALYSIS REPORT SAVED TO: {REPORT_FILE}"
)