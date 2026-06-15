import os
import numpy as np
import rasterio
from rasterio.transform import from_origin

os.makedirs(
    "assets/bands",
    exist_ok=True
)

width=300
height=300

transform=from_origin(
    19.0,
    51.0,
    10,
    10
)

red=np.random.normal(
    1200,
    250,
    (height,width)
)

nir=np.random.normal(
    2500,
    500,
    (height,width)
)

profile = {
    "driver":"GTiff",
    "height":height,
    "width":width,
    "count":1,
    "dtype":"float32",
    "crs":"EPSG:4326",
    "transform":transform
}

with rasterio.open(
    "assets/bands/B04_10m.tif",
    "w",
    **profile
) as dst:
    dst.write(red.astype("float32"), 1)

with rasterio.open(
    "assets/bands/B08_10m.tif",
    "w",
    **profile
) as dst:
    dst.write(nir.astype("float32"), 1)

print(
    "Sample bands created:"
)
print(
    "assets/bands/B04_10m.tif"
)
print(
    "assets/bands/B08_10m.tif"
)
