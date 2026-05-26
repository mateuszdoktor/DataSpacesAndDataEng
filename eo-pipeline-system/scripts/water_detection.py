import os
import numpy as np
import matplotlib.pyplot as plt

NDVI_PATH="results/ndvi/ndvi.npy"
MASK_OUTPUT="results/ndvi/water_mask.png"
MASK_ARRAY="results/ndvi/water_mask.npy"
REPORT_FILE="reports/water_detection.txt"

os.makedirs(
    "results/ndvi",
    exist_ok=True
)
os.makedirs(
    "reports",
    exist_ok=True
)

print(
    "WATER DETECTION"
)
print(
    "="*50
)
print(
    "Loading NDVI..."
)

ndvi=np.load(
    NDVI_PATH
)

print(
    "Generating binary mask..."
)

water_mask=ndvi<0
water_pixels=int(np.sum(water_mask))
non_water_pixels=int(np.sum(~water_mask))
water_percent=(water_pixels / ndvi.size) * 100

plt.figure(figsize=(10,8))
plt.imshow(water_mask)
plt.colorbar()
plt.savefig(MASK_OUTPUT)
plt.close()

np.save(MASK_ARRAY, water_mask)

with open(REPORT_FILE, "w") as f:
    f.write("WATER DETECTION REPORT\n")
    f.write("======================\n")
    f.write("Detection rule:\n")
    f.write("NDVI < 0\n")
    f.write("Pixel statistics:\n")
    f.write(f"Water candidate pixels: {water_pixels}\n")
    f.write(f"Non-water pixels: {non_water_pixels}\n")
    f.write(f"Water candidate percentage: {water_percent:.2f}%\n")
    f.write("Interpretation:\n")
    f.write("Small number of low-NDVI areas detected.\n")
    f.write("Potential explanations:\n")
    f.write("- water surfaces\n")
    f.write("- shadows\n")
    f.write("- urban regions\n")

print(
    "Water candidate pixels:",
    water_pixels
)
print(
    "Non-water pixels:",
    non_water_pixels
)
print(
    "FILES GENERATED:"
)
print(
    "---------------"
)
print(
    "results/ndvi/water_mask.npy"
)
print(
    "results/ndvi/water_mask.png"
)
print(
    "reports/water_detection.txt"
)
