import os
import json
import shutil

DATASET_DIR = "dataset"
IMAGES_DIR = "dataset/images"
METADATA_DIR = "dataset/metadata"

FILES_TO_INCLUDE = {
    "ndvi_map": "results/ndvi/ndvi_map.png",
    "water_mask": "results/ndvi/water_mask.png",
    "observation_low_cloud": "results/ndvi_comparison/low_cloud_observation_ndvi_map.png",
    "observation_high_cloud": "results/ndvi_comparison/high_cloud_observation_ndvi_map.png",
    "time_series_trend": "results/ndvi_timeseries/mean_ndvi_trend.png"
}

QUALITY_RULES = {
    "excellent": {
        "max_cloud_cover": 10,
        "min_ndvi": 0.5
    },
    "good": {
        "max_cloud_cover": 30,
        "min_ndvi": 0.3
    },
    "limited": {
        "max_cloud_cover": 70,
        "min_ndvi": 0.1
    }
}

def determine_quality(cloud_cover, mean_ndvi):
    if cloud_cover <= QUALITY_RULES["excellent"]["max_cloud_cover"] and mean_ndvi >= QUALITY_RULES["excellent"]["min_ndvi"]:
        return "excellent", "AI_READY"
    elif cloud_cover <= QUALITY_RULES["good"]["max_cloud_cover"] and mean_ndvi >= QUALITY_RULES["good"]["min_ndvi"]:
        return "good", "AI_READY"
    else:
        return "limited", "NOT_RECOMMENDED"

synthetic_observations = [
    {
        "id": "OBS_001",
        "cloud_cover": 5,
        "mean_ndvi": 0.62,
        "sensor": "Sentinel-2"
    },
    {
        "id": "OBS_002",
        "cloud_cover": 18,
        "mean_ndvi": 0.44,
        "sensor": "Sentinel-2"
    },
    {
        "id": "OBS_003",
        "cloud_cover": 68,
        "mean_ndvi": 0.21,
        "sensor": "Sentinel-2"
    }
]

def main():
    print("AI-READY EO DATASET PREPARATION")
    print("============================================================")
    
    os.makedirs(IMAGES_DIR, exist_ok=True)
    os.makedirs(METADATA_DIR, exist_ok=True)

    quality_distribution = {
        "excellent": 0,
        "good": 0,
        "limited": 0
    }

    for obs in synthetic_observations:
        obs_id = obs["id"]
        obs["selected_assets"] = []
        for key in ["ndvi_map", "water_mask"]:
            src = FILES_TO_INCLUDE.get(key)
            if src and os.path.exists(src):
                dst = f"{IMAGES_DIR}/{obs_id}_{key}.png"
                shutil.copy(src, dst)
                print(f"COPIED: {dst}")
                obs["selected_assets"].append(dst)
            else:
                print(f"MISSING: {src}")

    for key, src in FILES_TO_INCLUDE.items():
        if key not in ["ndvi_map", "water_mask"]:
            if src and os.path.exists(src):
                dst = f"{IMAGES_DIR}/{os.path.basename(src)}"
                shutil.copy(src, dst)
                print(f"COPIED: {dst}")
            else:
                print(f"MISSING: {src}")

    for obs in synthetic_observations:
        obs_id = obs["id"]
        quality, suitability = determine_quality(obs["cloud_cover"], obs["mean_ndvi"])
        quality_distribution[quality] += 1
        
        metadata = {
            "observation_id": obs_id,
            "sensor": obs["sensor"],
            "cloud_cover": obs["cloud_cover"],
            "mean_ndvi": obs["mean_ndvi"],
            "quality": quality,
            "suitability": suitability,
            "selected_assets": obs.get("selected_assets", []),
            "labels": {
                "vegetation_monitoring": suitability,
                "ai_training": suitability,
                "cloud_conditions": "LOW" if obs["cloud_cover"] < 30 else "HIGH"
            }
        }
        
        meta_file = f"{METADATA_DIR}/{obs_id}.json"
        with open(meta_file, "w") as f:
            json.dump(metadata, f, indent=4)
        print(f"METADATA CREATED: {meta_file}")

    print("DATASET SUMMARY")
    print("---------------------------------------")
    print(f"Dataset size: {len(synthetic_observations)}")
    print(f"Excellent: {quality_distribution['excellent']}")
    print(f"Good: {quality_distribution['good']}")
    print(f"Limited: {quality_distribution['limited']}")
    
    summary = {
        "dataset_size": len(synthetic_observations),
        "images_directory": IMAGES_DIR,
        "metadata_directory": METADATA_DIR,
        "quality_distribution": quality_distribution
    }
    with open(f"{METADATA_DIR}/dataset_summary.json", "w") as f:
        json.dump(summary, f, indent=4)
        
    print("GENERATED STRUCTURE:")
    print("------------------")
    print("dataset/")
    print("  images/")
    print("  metadata/")
    print("AI-READY DATASET COMPLETE")

if __name__ == "__main__":
    main()
