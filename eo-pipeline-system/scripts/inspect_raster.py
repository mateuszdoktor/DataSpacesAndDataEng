import os
import rasterio

RASTER_FILES=[
    "assets/visual/visual.jp2",
    "assets/bands/B04_10m.tif",
    "assets/bands/B08_10m.tif"
]

report_lines=[]

for raster_path in RASTER_FILES:
    print(
        "="*50
    )
    print(
        raster_path
    )
    report_lines.append(
        "="*50
    )
    report_lines.append(
        raster_path
    )
    
    if not os.path.exists(raster_path):
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
    
    try:
        with rasterio.open(raster_path) as src:
            print(
                "Width:",
                src.width
            )
            print(
                "Height:",
                src.height
            )
            print(
                "Bands:",
                src.count
            )
            print(
                "CRS:",
                src.crs
            )
            print(
                "Bounds:",
                src.bounds
            )
            
            report_lines.append(
                f"Width: {src.width}"
            )
            report_lines.append(
                f"Height: {src.height}"
            )
            report_lines.append(
                f"Bands: {src.count}"
            )
            report_lines.append(
                f"CRS: {src.crs}"
            )
            report_lines.append(
                f"Bounds: {src.bounds}"
            )
    except Exception as e:
        print(
            f"ERROR: {str(e)}"
        )
        report_lines.append(
            f"ERROR: {str(e)}"
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
    "reports/raster_inspection.txt",
    "w"
) as f:
    f.write(
        "\n".join(report_lines)
    )

print(
    "Report saved to: reports/raster_inspection.txt"
)