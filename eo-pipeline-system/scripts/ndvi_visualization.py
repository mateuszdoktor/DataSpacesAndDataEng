import numpy as np
ndvi = np.load(
    "results/ndvi/ndvi.npy"
)
print(
    "NDVI shape:",
    ndvi.shape
)
print(
    "NDVI min:",
    ndvi.min()
)
print(
    "NDVI max:",
    ndvi.max()
)

import matplotlib.pyplot as plt
plt.imshow(
    ndvi
)

plt.colorbar()

plt.savefig(
    "results/ndvi/ndvi_map.png"
)
plt.close()

high_vegetation_pixels = np.sum(
    ndvi > 0.5
)
low_ndvi_pixels = np.sum(
    ndvi < 0
)

total_pixels = ndvi.size

high_vegetation_percent = (
    high_vegetation_pixels / total_pixels
) * 100

low_ndvi_percent = (
    low_ndvi_pixels / total_pixels
) * 100
print(
    "High vegetation pixels:",
    high_vegetation_pixels
)
print(
    "Low NDVI pixels:",
    low_ndvi_pixels
)