import os
import numpy as np
import rasterio
import matplotlib.pyplot as plt

with rasterio.open(
    "assets/bands/B04_10m.tif"
) as red_src:
    red=red_src.read(1).astype(float)

with rasterio.open(
    "assets/bands/B08_10m.tif"
) as nir_src:
    nir=nir_src.read(1).astype(float)

ndvi=(nir-red)/(nir+red+1e-6)

ndvi_min=float(ndvi.min())
ndvi_max=float(ndvi.max())
ndvi_mean=float(ndvi.mean())

count_gt_05=int((ndvi>0.5).sum())
count_lt_0=int((ndvi<0).sum())

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

os.makedirs(
    "results/ndvi",
    exist_ok=True
)

np.save(
    "results/ndvi/ndvi.npy",
    ndvi
)

plt.figure(figsize=(6,6))
plt.imshow(ndvi, cmap="RdYlGn", vmin=-1, vmax=1)
plt.colorbar(label="NDVI")
plt.title("NDVI Map")
plt.tight_layout()
plt.savefig("results/ndvi/ndvi_map.png", dpi=150)
plt.close()

os.makedirs(
    "reports",
    exist_ok=True
)

with open(
    "reports/ndvi_report.txt",
    "w"
) as f:
    f.write(f"NDVI MIN: {ndvi_min}\n")
    f.write(f"NDVI MAX: {ndvi_max}\n")
    f.write(f"NDVI MEAN: {ndvi_mean}\n")
    f.write(f"NDVI > 0.5: {count_gt_05}\n")
    f.write(f"NDVI < 0: {count_lt_0}\n")

print(
    "NDVI array saved"
)
print(
    "NDVI map saved: results/ndvi/ndvi_map.png"
)
print(
    "Report saved: reports/ndvi_report.txt"
)
