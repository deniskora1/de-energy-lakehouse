import json
import time
from pathlib import Path

import requests

BASE = "https://www.smard.de/app/chart_data"
RAW_DIR = Path("data/raw/smard")

FILTERS = {
    4067: "wind_onshore",
    1225: "wind_offshore",
    4068: "photovoltaik",
    410: "netzlast",
    4169: "preis_de_lu",
}

REGION = "DE"
RESOLUTION = "hour"
WEEKS = 52

def get_json(url, retries=3, pause=2):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            print(f" error ({attempt}/{retries}): {e}")
            if attempt == retries:
                raise
            time.sleep(pause * attempt)

def fetch_index(filter_id):
    url = f"{BASE}/{filter_id}/{REGION}/index_{RESOLUTION}.json"
    return get_json(url)["timestamps"]

def fetch_timeseries(filter_id, ts):
    url = f"{BASE}/{filter_id}/{REGION}/{filter_id}_{REGION}_{RESOLUTION}_{ts}.json"
    return get_json(url)

def run():
    for filter_id, name in FILTERS.items():
        print(f"Filter {filter_id} ({name})")
        timestamps = fetch_index(filter_id)[-WEEKS:]
        out_dir = RAW_DIR / f"filter={filter_id}"
        out_dir.mkdir(parents=True, exist_ok=True)

        always_refresh = set(timestamps[-2:])

        for ts in timestamps:
            path = out_dir / f"ts={ts}.json"
            if path.exists() and ts not in always_refresh:
                continue
            data = fetch_timeseries(filter_id, ts)
            path.write_text(json.dumps(data), encoding="utf-8")
            print(f" saved {path.name} ({len(data["series"])} rows)")
            time.sleep(0.3)

if __name__ == "__main__":
    run()