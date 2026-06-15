import os
import numpy as np
import rasterio

RASTER_FILES={
    "B04_10m":"assets/bands/B04_10m.tif",
    "B08_10m":"assets/bands/B08_10m.tif"
}

report_lines=[]

for name, path in RASTER_FILES.items():
    print(
        "="*50
    )
    print(
        name
    )
    report_lines.append(
        "="*50
    )
    report_lines.append(
        name
    )

    if not os.path.exists(path):
        print(
            "SKIPPED: File not found"
        )
        report_lines.append(
            "SKIPPED: File not found"
        )
        print()
        report_lines.append(
            ""
        )
        continue

    with rasterio.open(path) as src:
        band=src.read(1)

    min_val=float(np.min(band))
    max_val=float(np.max(band))
    mean_val=float(np.mean(band))
    std_val=float(np.std(band))

    print(
        "MIN:",
        min_val
    )
    print(
        "MAX:",
        max_val
    )
    print(
        "MEAN:",
        mean_val
    )
    print(
        "STD:",
        std_val
    )

    report_lines.append(
        f"MIN: {min_val}"
    )
    report_lines.append(
        f"MAX: {max_val}"
    )
    report_lines.append(
        f"MEAN: {mean_val}"
    )
    report_lines.append(
        f"STD: {std_val}"
    )
    print()
    report_lines.append(
        ""
    )

os.makedirs(
    "reports",
    exist_ok=True
)

with open(
    "reports/spectral_statistics.txt",
    "w"
) as f:
    f.write(
        "\n".join(report_lines)
    )

print(
    "Report saved to: reports/spectral_statistics.txt"
)
