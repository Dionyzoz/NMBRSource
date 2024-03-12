import json


configured_catalog = {"streams": []}
with open("catalog.json", "r") as file:
    streams = json.load(file)["catalog"]["streams"]
    for stream in streams:
        configured_catalog["streams"].append({
            "stream": {
                "name": stream["name"],
                "json_schema": {},
                "supported_sync_modes": ["full_refresh"],
            },
            "sync_mode": "full_refresh",
            "destination_sync_mode": "overwrite",
        })

with open("sample_files/configured_catalog.json", "w") as file:
    json.dump(configured_catalog, file)
